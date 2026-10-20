import { listDurableObjectIds, SELF } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import type { D1Migration } from "@cloudflare/vitest-plugin";
import { syntheticCredentials } from "./fixtures/synthetic-credentials";
import { ACTION_QUEUE_LIMIT, appendAction, removeQueuedAction, shouldRetryActionFailure } from "../public/file/action-queue.js";
import { HttpResponseError } from "../public/file/http-error.js";
import { filterPresentLegacyCandidates, getOrCreateLegacySelection, updateLegacySelection } from "../public/file/legacy-mapping-state.js";
import { submitLegacyFileImport } from "../public/file/legacy-file-import.js";
import { hasMetadataPatchFields, hasMetadataPreviewChanges, metadataPreviewDetails } from "../public/file/metadata-preview.js";
import { MAX_SELECTED_ITEMS, updateSelectionWithinLimit } from "../public/file/selection-limit.js";
import {
  buildMigrationPreviewRefreshPayload,
  migrationConflictChoicesChanged,
  migrationControlActions,
  migrationHistoryNotice,
  migrationRefreshMatches,
  setMigrationConflictChoice,
} from "../public/file/migration-conflicts.js";
import {
  MAX_TARGET_RELATIVE_DIR_BYTES as CLIENT_MAX_TARGET_RELATIVE_DIR_BYTES,
  MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES as CLIENT_MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES,
  validateTargetRelativeDirectory,
} from "../public/file/target-directory.js";
import { renderMovePreviewItems } from "../public/file/preview-rendering.js";
import { acceptOwnerSnapshot, beginOwnerVerification, drainActionsAfterSnapshotRefresh, failOwnerSnapshot, mayDispatchActions, mayUsePrivateSnapshot } from "../public/file/snapshot-session.js";
import { sourceIdUrl } from "../public/file/source-links.js";
import { buildWaitingEntries, isIncompletePreview, isPresentMedia, isReviewablePreviewStatus, isWaitingEntryPending, mergeRequestEntries, operationDetailsWarning, operationHistoryEntries, presentMediaItems } from "../public/file/state-view.js";
import { MAX_MIGRATION_TASK_ROWS, incompleteMigrationTaskSummary } from "../public/file/migration-task-summary.js";
import { canOperateOnMediaLibrary, resolveMediaLibrary } from "../public/file/library-view.js";
import {
  canonicalJson,
  MAX_ACTION_BODY_BYTES,
  MAX_LEGACY_TEXT_BYTES,
  MAX_MIGRATION_ITEM_CONFLICTS as WORKER_MAX_MIGRATION_ITEM_CONFLICTS,
  MAX_SELECTED_ITEMS as WORKER_MAX_SELECTED_ITEMS,
  MAX_STATE_BODY_BYTES,
  MAX_TARGET_RELATIVE_DIR_BYTES as WORKER_MAX_TARGET_RELATIVE_DIR_BYTES,
  MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES as WORKER_MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES,
  readJsonBody,
  validateSnapshot,
} from "../src/validation";
import { ACKED_INBOX_PRUNE_BATCH_SIZE, ACKED_INBOX_REPLAY_RETENTION_MS, enqueue, getDatabaseEpoch, storeSnapshot } from "../src/store";
import type { LocalSnapshot } from "../src/types";
import worker from "../src/index";

const testEnv = env as typeof env & { TEST_MIGRATIONS: D1Migration[] };
const ownerId = 123456789;
const ownerToken = syntheticCredentials.localSharedSecret.value;
const webhookToken = syntheticCredentials.webhookSecret.value;
const botToken = syntheticCredentials.botToken.value;
const workerUrl = "https://worker.test";
const textEncoder = new TextEncoder();
const testDatabaseEpoch = "0123456789abcdef0123456789abcdef";

describe("incomplete migration task summary", () => {
  const tasks = [
    { kind: "video", library_id: "source", status: "queued", staging_bytes: 10 },
    { kind: "video", library_id: "source", status: "failed", partial_bytes: 20 },
    { kind: "video", library_id: "source", status: "interrupted", downloaded_bytes: 30 },
    { kind: "video", destination: { library_id: "source" }, status: "failed", staging_bytes: 40 },
    { kind: "video", library_id: "other", status: "queued", staging_bytes: 50 },
    { kind: "pdf", library_id: "source", status: "queued", staging_bytes: 60 },
    { kind: "video", library_id: "source", status: "completed", staging_bytes: 70 },
    { kind: "video", library_id: "source", status: "cancelled", staging_bytes: 80 },
  ];

  it("counts only unfinished video tasks frozen to the selected source library", () => {
    expect(incompleteMigrationTaskSummary({ id: "source", incomplete_task_count: null, staging_bytes: null }, tasks)).toEqual({
      count: 4,
      bytes: 100,
      countIsLowerBound: false,
    });
  });

  it("treats a reported zero as authoritative while missing or null fields use task rows", () => {
    expect(incompleteMigrationTaskSummary({ id: "source", incomplete_task_count: 0, staging_bytes: 0 }, tasks)).toEqual({
      count: 0,
      bytes: 0,
      countIsLowerBound: false,
    });
    expect(incompleteMigrationTaskSummary({ id: "source" }, tasks).count).toBe(4);
    expect(incompleteMigrationTaskSummary({ id: "source", incomplete_task_count: null }, tasks).count).toBe(4);
    expect(incompleteMigrationTaskSummary({ id: "source", incomplete_task_count: "0" }, tasks).count).toBe(4);
  });

  it("does not present missing or incomplete staged-byte estimates as zero", () => {
    const rows = [{ kind: "video", library_id: "source", status: "failed" }];
    expect(incompleteMigrationTaskSummary({ id: "source", incomplete_task_count: 1, staging_bytes: null }, rows).bytes).toBeNull();
    expect(incompleteMigrationTaskSummary({ id: "source" }, rows).bytes).toBeNull();
    expect(incompleteMigrationTaskSummary({ id: "source" }, []).bytes).toBe(0);
  });

  it("caps fallback work to the bounded task projection and marks a lower-bound count", () => {
    const rows = Array.from({ length: MAX_MIGRATION_TASK_ROWS + 1 }, () => ({
      kind: "video",
      library_id: "source",
      status: "queued",
      staging_bytes: 1,
    }));
    expect(incompleteMigrationTaskSummary({ id: "source" }, rows)).toEqual({
      count: MAX_MIGRATION_TASK_ROWS,
      bytes: null,
      countIsLowerBound: true,
    });
  });
});

describe("metadata preview details", () => {
  it("shows only concrete known-field changes and the source IDs to add", () => {
    expect(metadataPreviewDetails({
      changed: true,
      changes: [
        { field: "title", before: "Old title", after: "Confirmed title" },
        { field: "collection", before: null, after: "Series" },
        { field: "order", before: "1", after: "2" },
      ],
      added_source_ids: ["BV1abcdefgh", "cid11"],
    })).toEqual([
      { kind: "status", text: "NFO will be updated" },
      { kind: "change", label: "Title", before: "Old title", after: "Confirmed title" },
      { kind: "change", label: "Collection", before: null, after: "Series" },
      { kind: "change", label: "Order", before: "1", after: "2" },
      { kind: "change", label: "Source IDs to add", before: null, after: "BV1abcdefgh, cid11" },
    ]);
  });

  it("recognizes one actual metadata field without counting item identity fields", () => {
    expect(hasMetadataPatchFields({ item_id: "video", source_ids: ["youtube:dQw4w9WgXcQ"] })).toBe(true);
    expect(hasMetadataPatchFields({ item_id: "video", hint_index: 3 })).toBe(false);
  });

  it("detects actual metadata changes instead of treating an empty patch list as an update", () => {
    expect(hasMetadataPreviewChanges({ items: [{ item_id: "video" }], metadata_patches: [] })).toBe(false);
    expect(hasMetadataPreviewChanges({ metadata_patches: [{ changes: [{ field: "title", before: "Old", after: "New" }] }] })).toBe(true);
  });
});

describe("move preview rendering", () => {
  it("renders every media and related-file source-to-destination path in the confirmation preview", () => {
    const rendered = renderMovePreviewItems([{
      item_id: "movie",
      source_path: "Inbox/movie.mkv",
      target_path: "Library/movie.mkv",
      outcome: "move",
      files: [
        { source_path: "Inbox/movie.mkv", target_path: "Library/movie.mkv" },
        { source_path: "Inbox/movie.en.srt", target_path: "Library/movie.en.srt" },
        { source_path: "Inbox/movie-poster.jpg", target_path: "Library/movie-poster.jpg" },
        { source_path: "Inbox/movie.nfo", target_path: "Library/movie.nfo" },
      ],
    }]);

    expect(rendered).toContain('<details class="preview-file-details" open>');
    expect(rendered).toContain("Review 4 exact source → destination paths");
    for (const [source, target] of [
      ["Inbox/movie.mkv", "Library/movie.mkv"],
      ["Inbox/movie.en.srt", "Library/movie.en.srt"],
      ["Inbox/movie-poster.jpg", "Library/movie-poster.jpg"],
      ["Inbox/movie.nfo", "Library/movie.nfo"],
    ]) {
      expect(rendered).toContain(`${source} → ${target}`);
    }
  });

  it("HTML-escapes item and file paths before placing them in preview markup", () => {
    const hostile = '<img src=x onerror="alert(1)"> & movie.mkv';
    const rendered = renderMovePreviewItems([{
      source_path: hostile,
      target_path: hostile,
      files: [{ source_path: hostile, target_path: hostile }],
    }]);

    expect(rendered).not.toContain("<img");
    expect(rendered).toContain("&lt;img src=x onerror=&quot;alert(1)&quot;&gt; &amp; movie.mkv");
  });
});

describe("migration conflict choices", () => {
  it("marks changed bundle choices stale and binds refreshed choices to the new preview", () => {
    const preview = {
      id: "migration-preview-old",
      revision: "revision-a",
      source_library_id: "library-source",
      target_library_id: "library-target",
      scope: "library",
      item_ids: ["item-a", "item-b"],
      include_incomplete_tasks: true,
      conflict: "skip",
      item_conflicts: {},
      set_target_default_on_success: false,
      items: [{ item_id: "item-a", conflict: true }],
    };
    const draft = setMigrationConflictChoice(preview.item_conflicts, "item-a", "keep_both", "skip");
    expect(migrationConflictChoicesChanged(preview.item_conflicts, draft)).toBe(true);
    const refresh = buildMigrationPreviewRefreshPayload(preview, draft, {
      seq: 21,
      id: preview.id,
      revision: preview.revision,
    });
    expect(refresh).toEqual({
      source_library_id: "library-source",
      target_library_id: "library-target",
      scope: "library",
      include_incomplete_tasks: true,
      conflict: "skip",
      set_target_default_on_success: false,
      supersedes_preview_seq: 21,
      supersedes_preview_id: "migration-preview-old",
      supersedes_revision: "revision-a",
      item_conflicts: { "item-a": "keep_both" },
    });
    expect(migrationRefreshMatches({ ...preview, id: "migration-preview-new", item_conflicts: draft }, refresh)).toBe(true);
    expect(migrationRefreshMatches({ ...preview, id: "migration-preview-new", item_conflicts: {} }, refresh)).toBe(false);
  });

  it("omits overrides for non-conflicting bundles and restores the global choice cleanly", () => {
    const changed = setMigrationConflictChoice({}, "item-a", "keep_both", "skip");
    const restored = setMigrationConflictChoice(changed, "item-a", "skip", "skip");
    expect(restored).toEqual({});
    const refresh = buildMigrationPreviewRefreshPayload({
      source_library_id: "library-source",
      target_library_id: "library-target",
      scope: "selected",
      id: "migration-preview-selected",
      revision: "revision-selected",
      item_ids: ["item-a", "item-b"],
      conflict: "skip",
      items: [{ item_id: "item-a", conflict: false }, { item_id: "item-b", conflict: true }],
    }, { "item-a": "keep_both", "item-b": "keep_both" }, { seq: 22, id: "migration-preview-selected", revision: "revision-selected" });
    expect(refresh?.item_conflicts).toEqual({ "item-b": "keep_both" });
    expect(refresh).toMatchObject({
      supersedes_preview_seq: 22,
      supersedes_preview_id: "migration-preview-selected",
      supersedes_revision: "revision-selected",
    });
  });

  it("requires the exact current preview identity before making a replacement preview", () => {
    expect(buildMigrationPreviewRefreshPayload({
      source_library_id: "library-source",
      target_library_id: "library-target",
      scope: "library",
    }, {}, { seq: 0, id: "old", revision: "rev" })).toBeNull();
    expect(buildMigrationPreviewRefreshPayload({
      source_library_id: "library-source",
      target_library_id: "library-target",
      scope: "library",
    }, {}, { seq: 1, id: "", revision: "rev" })).toBeNull();
  });

  it("preserves the inbox AUTOINCREMENT high-water mark when a migration rebuild copies no rows", async () => {
    await testEnv.DB.prepare(`CREATE TABLE inbox_probe (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      dedup_key TEXT NOT NULL UNIQUE,
      payload_hash TEXT NOT NULL,
      kind TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      acked_at INTEGER
    )`).run();
    await testEnv.DB.prepare(`INSERT INTO inbox_probe (seq, dedup_key, payload_hash, kind, payload_json, created_at)
      VALUES (6, 'probe:6', 'hash-6', 'library_scan', '{}', 1),
             (47, 'probe:47', 'hash-47', 'library_scan', '{}', 1)`).run();
    await testEnv.DB.prepare("DELETE FROM inbox_probe").run();

    const migration = testEnv.TEST_MIGRATIONS.find(({ name }) => name.includes("0003_media_library_request_kinds"));
    expect(migration).toBeDefined();
    for (const query of migration?.queries ?? []) {
      const probeQuery = query
        .replaceAll("inbox_with_media_library_kinds", "inbox_probe_with_media_library_kinds")
        .replaceAll("inbox_sequence_backup", "inbox_probe_sequence_backup")
        .replaceAll("inbox_pending_seq", "inbox_probe_pending_seq")
        .replace(/\binbox\b/gu, "inbox_probe");
      await testEnv.DB.prepare(probeQuery).run();
    }

    const savedSequence = await testEnv.DB.prepare("SELECT seq FROM sqlite_sequence WHERE name = 'inbox_probe'").first<{ seq: number }>();
    expect(savedSequence?.seq).toBe(47);
    const next = await testEnv.DB.prepare("INSERT INTO inbox_probe (dedup_key, payload_hash, kind, payload_json, created_at) VALUES ('probe:next', 'hash-next', 'library_scan', '{}', 2) RETURNING seq").first<{ seq: number }>();
    expect(next?.seq).toBeGreaterThan(47);
  });
});

describe("migration progress controls and history notice", () => {
  it("offers recovery controls for task-transfer errors even when no media bundle failed", () => {
    expect(migrationControlActions({
      status: "partial",
      failed_count: 0,
      task_transfers: [{ error: "queue handoff needs recovery" }],
    })).toEqual(["stop_remaining", "retry_failed"]);
    expect(migrationControlActions({
      status: "failed",
      failed_count: 0,
      task_transfers: [{ error: "staged payload transfer failed" }],
    })).toEqual(["stop_remaining", "retry_failed"]);
  });

  it("keeps ordinary progress controls and hides terminal controls without recoverable work", () => {
    expect(migrationControlActions({ status: "running", failed_count: 0 })).toEqual(["pause", "stop_remaining"]);
    expect(migrationControlActions({ status: "paused", failed_count: 0 })).toEqual(["resume", "stop_remaining"]);
    expect(migrationControlActions({ status: "partial", failed_count: 0, task_transfers: [] })).toEqual([]);
    expect(migrationControlActions({ status: "partial", failed_count: 2 })).toEqual(["retry_failed"]);
  });

  it("shows a positive omitted-history count with wording for any capped migrations", () => {
    expect(migrationHistoryNotice(0)).toBe("");
    expect(migrationHistoryNotice(-1)).toBe("");
    expect(migrationHistoryNotice(32)).toBe("32 additional migrations are not included in this view; local recovery records are retained.");
    expect(migrationHistoryNotice(1)).toBe("1 additional migration is not included in this view; local recovery records are retained.");
  });
});

describe("target directory validation", () => {
  it("matches the Worker UTF-8 byte limit and keeps the root destination valid", () => {
    expect(CLIENT_MAX_TARGET_RELATIVE_DIR_BYTES).toBe(WORKER_MAX_TARGET_RELATIVE_DIR_BYTES);
    expect(validateTargetRelativeDirectory("")).toEqual({ directory: "", error: null });

    const cjkSegment = "界".repeat(70);
    const exact = [cjkSegment, cjkSegment, cjkSegment, cjkSegment, "x".repeat(180)].join("/");
    const oversized = [cjkSegment, cjkSegment, cjkSegment, cjkSegment, "x".repeat(181)].join("/");
    expect(textEncoder.encode(exact).byteLength).toBe(1024);
    expect(textEncoder.encode(oversized).byteLength).toBe(1025);
    expect(validateTargetRelativeDirectory(exact)).toEqual({ directory: exact, error: null });
    expect(validateTargetRelativeDirectory(oversized).error).toContain("1,024 UTF-8 bytes");

    const sixLevelSeventyCjk = Array.from({ length: 6 }, () => cjkSegment).join("/");
    expect(textEncoder.encode(sixLevelSeventyCjk).byteLength).toBe(1265);
    expect(validateTargetRelativeDirectory(sixLevelSeventyCjk).error).toContain("1,024 UTF-8 bytes");
  });

  it("matches the 255-byte component limit for ASCII, CJK, emoji, and a valid 1,023-byte path", () => {
    expect(CLIENT_MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES).toBe(
      WORKER_MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES,
    );

    const componentCases = [
      ["x".repeat(255), "x".repeat(256)],
      ["界".repeat(85), `${"界".repeat(85)}a`],
      [`${"😀".repeat(63)}abc`, `${"😀".repeat(63)}abcd`],
    ];
    for (const [accepted, rejected] of componentCases) {
      expect(textEncoder.encode(accepted).byteLength).toBe(255);
      expect(validateTargetRelativeDirectory(accepted)).toEqual({ directory: accepted, error: null });
      expect(textEncoder.encode(rejected).byteLength).toBe(256);
      expect(validateTargetRelativeDirectory(rejected).error).toContain("255 UTF-8 bytes");
    }

    const maxComponentsPath = Array.from({ length: 4 }, () => "x".repeat(255)).join("/");
    expect(textEncoder.encode(maxComponentsPath).byteLength).toBe(1_023);
    expect(validateTargetRelativeDirectory(maxComponentsPath)).toEqual({ directory: maxComponentsPath, error: null });
  });

  it("keeps control, malformed Unicode, and private path checks in the browser validator", () => {
    expect(validateTargetRelativeDirectory("Movies/Title\nPart").error).toContain("control characters");
    expect(validateTargetRelativeDirectory("Movies/\uD800").error).toContain("valid Unicode");
    expect(validateTargetRelativeDirectory("Movies/.TELEGRAM-VIDEO-DOWNLOADER-LIBRARY/cache").error).toContain("safe relative folder path");
    expect(validateTargetRelativeDirectory("Movies/../private").error).toContain("safe relative folder path");
  });
});

describe("selection limit", () => {
  it("matches the Worker request limit and preserves order while capping additions", () => {
    expect(MAX_SELECTED_ITEMS).toBe(WORKER_MAX_SELECTED_ITEMS);
    const added = updateSelectionWithinLimit(
      ["already-selected"],
      Array.from({ length: MAX_SELECTED_ITEMS }, (_, index) => `item-${index}`),
      true,
    );
    expect(added.ids).toHaveLength(MAX_SELECTED_ITEMS);
    expect(added.ids[0]).toBe("already-selected");
    expect(added.ids.at(-1)).toBe(`item-${MAX_SELECTED_ITEMS - 2}`);
    expect(added.omittedCount).toBe(1);

    const removed = updateSelectionWithinLimit(added.ids, ["already-selected"], false);
    expect(removed.ids).toHaveLength(MAX_SELECTED_ITEMS - 1);
    expect(removed.ids[0]).toBe("item-0");
    expect(removed.omittedCount).toBe(0);
  });
});

describe("legacy file import bounds", () => {
  it("rejects oversized files before reading or submitting them", async () => {
    const file = { size: 256 * 1024 + 1, text: vi.fn() };
    const submit = vi.fn();
    const reject = vi.fn();

    await submitLegacyFileImport(file, submit, reject, 256 * 1024);

    expect(file.text).not.toHaveBeenCalled();
    expect(submit).not.toHaveBeenCalled();
    expect(reject).toHaveBeenCalledWith("Legacy text exceeds the 256 KiB limit.");
  });

  it("accepts the exact file-size boundary and still checks decoded UTF-8 size", async () => {
    const limit = 256 * 1024;
    const submit = vi.fn();
    const reject = vi.fn();
    const file = { size: limit, text: vi.fn().mockResolvedValue("x".repeat(limit)) };

    await submitLegacyFileImport(file, submit, reject, limit);

    expect(file.text).toHaveBeenCalledOnce();
    expect(submit).toHaveBeenCalledWith("x".repeat(limit));
    expect(reject).not.toHaveBeenCalled();

    const decodedOversize = {
      size: limit,
      text: vi.fn().mockResolvedValue("é".repeat(limit / 2 + 1)),
    };
    const decodedSubmit = vi.fn();
    const decodedReject = vi.fn();
    await submitLegacyFileImport(decodedOversize, decodedSubmit, decodedReject, limit);
    expect(decodedOversize.text).toHaveBeenCalledOnce();
    expect(decodedSubmit).not.toHaveBeenCalled();
    expect(decodedReject).toHaveBeenCalledWith("Legacy text exceeds the 256 KiB limit.");
  });

  it("reads the maximum decoded legacy text at six-byte JSON escape expansion within the fixed action envelope", async () => {
    const escapedText = "\\u0078".repeat(MAX_LEGACY_TEXT_BYTES);
    const unpaddedBody = `{"request_id":"00000000-0000-4000-8000-000000009301","kind":"legacy_import","payload":{"text":"${escapedText}"}}`;
    const unpaddedBytes = textEncoder.encode(unpaddedBody).byteLength;
    expect(MAX_ACTION_BODY_BYTES).toBe(MAX_LEGACY_TEXT_BYTES * 6 + 4_096);
    expect(unpaddedBytes).toBeGreaterThan(MAX_LEGACY_TEXT_BYTES * 6);
    expect(unpaddedBytes).toBeLessThanOrEqual(MAX_ACTION_BODY_BYTES);

    const bodyAtLimit = unpaddedBody + " ".repeat(MAX_ACTION_BODY_BYTES - unpaddedBytes);
    expect(textEncoder.encode(bodyAtLimit).byteLength).toBe(MAX_ACTION_BODY_BYTES);
    const parsed = await readJsonBody(new Request(workerUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: bodyAtLimit,
    }), MAX_ACTION_BODY_BYTES) as { payload: { text: string } };
    expect(parsed.payload.text).toHaveLength(MAX_LEGACY_TEXT_BYTES);
    expect(parsed.payload.text[0]).toBe("x");

    await expect(readJsonBody(new Request(workerUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: `${bodyAtLimit} `,
    }), MAX_ACTION_BODY_BYTES)).rejects.toMatchObject({ status: 413 });
  });
});

describe("snapshot canonicalization bounds", () => {
  it("keeps the default canonical JSON node cap for non-snapshot payloads", () => {
    let error: unknown;
    try {
      canonicalJson(Array.from({ length: 100_000 }, () => null));
    } catch (caught) {
      error = caught;
    }
    expect(error).toMatchObject({ status: 400, message: "JSON structure is too complex" });
  });
});

describe("owner snapshot session", () => {
  const snapshotLibraries = (session: { snapshot: unknown }) => {
    const snapshot = session.snapshot as { library?: { libraries?: Array<Record<string, unknown>> } } | null;
    return snapshot?.library?.libraries ?? [];
  };

  it("keeps private UI and queued actions gated until a successful owner response", () => {
    const startup = beginOwnerVerification();
    expect(startup).toMatchObject({
      ownerVerified: false,
      snapshot: null,
      privateUiVisible: false,
      canDispatchActions: false,
      clearCache: true,
    });
    expect(mayUsePrivateSnapshot(startup)).toBe(false);
    expect(mayDispatchActions(startup)).toBe(false);

    expect(mayUsePrivateSnapshot({ ...startup, snapshot: { library: { items: ["private"] } } })).toBe(false);
    const accepted = acceptOwnerSnapshot({ synced: true, library: { items: ["private"] } });
    expect(mayUsePrivateSnapshot(accepted)).toBe(true);
    expect(mayDispatchActions(accepted)).toBe(true);
  });

  it.each([401, 403])("clears private state and pauses use after HTTP %i", (status) => {
    const accepted = acceptOwnerSnapshot({ synced: true, library: { items: ["private"] } });
    const failed = failOwnerSnapshot(accepted, status, 1234);
    expect(failed).toEqual({
      ownerVerified: false,
      snapshot: null,
      privateUiVisible: false,
      canDispatchActions: false,
      clearCache: true,
      authorizationRejected: true,
    });
    expect(mayUsePrivateSnapshot(failed)).toBe(false);
    expect(mayDispatchActions(failed)).toBe(false);
  });

  it("uses only the in-memory owner-verified snapshot after a temporary failure", () => {
    const accepted = acceptOwnerSnapshot({ synced: true, library: { items: ["private"] } });
    const stale = failOwnerSnapshot(accepted, 503, 1234);
    expect(stale.ownerVerified).toBe(true);
    expect(mayUsePrivateSnapshot(stale)).toBe(true);
    expect(mayDispatchActions(stale)).toBe(true);
    expect(stale.snapshot).toMatchObject({ stale: true, cached_at: 1234 });

    const firstLoadFailure = failOwnerSnapshot(beginOwnerVerification(), 503, 1234);
    expect(firstLoadFailure.snapshot).toBeNull();
    expect(mayUsePrivateSnapshot(firstLoadFailure)).toBe(false);
    expect(mayDispatchActions(firstLoadFailure)).toBe(false);
  });

  it("retains a stale per-library view only for the same verified owner, ID, and root", () => {
    const previous = acceptOwnerSnapshot({
      settings: {
        applied: {
          default_library_id: "main",
          libraries: [
            { id: "main", root: "/Volumes/Main" },
            { id: "archive", root: "/Volumes/Archive" },
            { id: "moved", root: "/Volumes/OldArchive" },
          ],
        },
      },
      library: {
        default_library_id: "main",
        libraries: [
          { id: "main", root: "/Volumes/Main", availability: "available", snapshot: { ready: true, items: [] } },
          { id: "archive", root: "/Volumes/Archive", availability: "available", snapshot: { ready: true, scanned_at: 123, items: [{ id: "cached", presence: "present" }] } },
          { id: "moved", root: "/Volumes/OldArchive", availability: "available", snapshot: { ready: true, items: [{ id: "old-root", presence: "present" }] } },
        ],
      },
    });
    const incoming = {
      synced: true,
      settings: {
        applied: {
          default_library_id: "main",
          libraries: [
            { id: "main", root: "/Volumes/Main", name: "Main" },
            { id: "archive", root: "/Volumes/Archive", name: "Renamed archive" },
            { id: "moved", root: "/Volumes/NewArchive", name: "Moved archive" },
          ],
        },
      },
      library: {
        default_library_id: "main",
        libraries: [
          { id: "main", root: "/Volumes/Main", availability: "available", snapshot: { ready: true, items: [{ id: "fresh", presence: "present" }] } },
          { id: "archive", root: "/Volumes/Archive", availability: "offline", media_count: null, snapshot: null },
          { id: "moved", root: "/Volumes/NewArchive", availability: "offline", media_count: null, snapshot: null },
        ],
      },
    };

    const accepted = acceptOwnerSnapshot(incoming, previous);
    const libraries = snapshotLibraries(accepted);
    expect(libraries.find((entry) => entry.id === "main")?.snapshot).toMatchObject({
      items: [
      { id: "fresh", presence: "present" },
      ],
    });
    expect(libraries.find((entry) => entry.id === "archive")).toMatchObject({
      root: "/Volumes/Archive",
      availability: "offline",
      snapshot_stale: true,
      snapshot: {
        stale: true,
        scanned_at: 123,
        items: [{ id: "cached", presence: "unavailable" }],
      },
    });
    const cachedArchive = resolveMediaLibrary(accepted.snapshot, "archive");
    expect(cachedArchive.snapshot?.items).toMatchObject([{ id: "cached", presence: "unavailable" }]);
    expect(canOperateOnMediaLibrary(cachedArchive.descriptor)).toBe(false);
    expect(libraries.find((entry) => entry.id === "moved")).toMatchObject({
      root: "/Volumes/NewArchive",
      snapshot: null,
    });
    expect(libraries.find((entry) => entry.id === "moved")?.snapshot_stale).toBeUndefined();

    const notVerified = { ...beginOwnerVerification(), snapshot: previous.snapshot };
    const isolated = acceptOwnerSnapshot(incoming, notVerified);
    expect(snapshotLibraries(isolated).find((entry) => entry.id === "archive")?.snapshot).toBeNull();
  });

  it("replaces a retained library view when that same root reports a ready scan", () => {
    const previous = acceptOwnerSnapshot({
      settings: { applied: { default_library_id: "archive", libraries: [{ id: "archive", root: "/Volumes/Archive" }] } },
      library: {
        default_library_id: "archive",
        libraries: [{ id: "archive", root: "/Volumes/Archive", availability: "available", snapshot: { ready: true, scanned_at: 123, items: [{ id: "old", presence: "present" }] } }],
      },
    });
    const offline = acceptOwnerSnapshot({
      settings: { applied: { default_library_id: "archive", libraries: [{ id: "archive", root: "/Volumes/Archive" }] } },
      library: {
        default_library_id: "archive",
        libraries: [{ id: "archive", root: "/Volumes/Archive", availability: "offline", snapshot: null }],
      },
    }, previous);
    const recovered = acceptOwnerSnapshot({
      settings: { applied: { default_library_id: "archive", libraries: [{ id: "archive", root: "/Volumes/Archive" }] } },
      library: {
        default_library_id: "archive",
        libraries: [{ id: "archive", root: "/Volumes/Archive", availability: "available", snapshot: { ready: true, scanned_at: 456, items: [{ id: "new", presence: "present" }] } }],
      },
    }, offline);

    expect(snapshotLibraries(recovered)[0]).toMatchObject({
      availability: "available",
      snapshot: { scanned_at: 456, items: [{ id: "new", presence: "present" }] },
    });
    expect(snapshotLibraries(recovered)[0]?.snapshot_stale).toBeUndefined();
    expect(canOperateOnMediaLibrary(snapshotLibraries(recovered)[0] as { id: string; availability?: string; snapshot_stale?: boolean })).toBe(true);
  });

  it("keeps older unknown-availability descriptors operable for compatibility", () => {
    expect(canOperateOnMediaLibrary({ id: "legacy" })).toBe(true);
    expect(canOperateOnMediaLibrary({ id: "offline", availability: "unavailable" })).toBe(false);
    expect(canOperateOnMediaLibrary({ id: "stale", snapshot_stale: true })).toBe(false);
  });

  it("drains saved actions after a quiet poll recovers owner verification", async () => {
    const failedInitialLoad = failOwnerSnapshot(beginOwnerVerification(), 503, 1234);
    const drain = vi.fn().mockResolvedValue(true);
    expect(await drainActionsAfterSnapshotRefresh(failedInitialLoad, true, drain)).toBe(false);
    expect(drain).not.toHaveBeenCalled();

    const recovered = acceptOwnerSnapshot({ synced: true, library: { items: [] } });
    expect(await drainActionsAfterSnapshotRefresh(recovered, true, drain)).toBe(true);
    expect(drain).toHaveBeenCalledOnce();
    expect(await drainActionsAfterSnapshotRefresh(recovered, false, drain)).toBe(false);
    expect(drain).toHaveBeenCalledOnce();
  });
});

describe("Mini App source links", () => {
  it("links canonical YouTube IDs and leaves other IDs unchanged", () => {
    expect(sourceIdUrl("youtube:dQw4w9WgXcQ")).toBe("https://www.youtube.com/watch?v=dQw4w9WgXcQ");
    expect(sourceIdUrl("BV17x411w7KC")).toBeNull();
    expect(sourceIdUrl("youtube:bad/id")).toBeNull();
  });
});

function ownerMessage(updateId: number, text = "/scan", fromId = ownerId, chatId = ownerId, chatType = "private") {
  return {
    update_id: updateId,
    message: {
      message_id: updateId,
      date: Math.floor(Date.now() / 1000),
      from: { id: fromId, is_bot: false, first_name: "Owner" },
      chat: { id: chatId, type: chatType, first_name: "Owner" },
      text,
    },
  };
}

async function webhook(update: unknown, secret: string = webhookToken): Promise<Response> {
  return SELF.fetch(new Request(`${workerUrl}/webhook`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-telegram-bot-api-secret-token": secret },
    body: JSON.stringify(update),
  }));
}

async function webhookWithOwnerId(update: unknown, ownerUserId: string, secret: string = webhookToken): Promise<Response> {
  const overriddenEnv = Object.create(testEnv) as object;
  Object.defineProperty(overriddenEnv, "OWNER_USER_ID", { value: ownerUserId });
  return worker.fetch(
    new Request(`${workerUrl}/webhook`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-telegram-bot-api-secret-token": secret },
      body: JSON.stringify(update),
    }),
    overriddenEnv as Env,
  );
}

function localHeaders(): HeadersInit {
  return {
    authorization: `Bearer ${ownerToken}`,
    "X-Cloud-Database-Epoch": testDatabaseEpoch,
  };
}

async function responseJson<T>(response: Response): Promise<T> {
  return await response.json() as T;
}

async function localRequests(limit = 100): Promise<Response> {
  return SELF.fetch(new Request(`${workerUrl}/api/local/requests?limit=${limit}`, { headers: localHeaders() }));
}

async function acknowledgmentBody(seqs: number[]): Promise<{ receipts: Array<Record<string, unknown>> }> {
  const rows: Array<{
    seq: number;
    dedup_key: string;
    payload_hash: string;
    kind: string;
    created_at: number;
  }> = [];
  for (let offset = 0; offset < seqs.length; offset += 90) {
    const chunk = seqs.slice(offset, offset + 90);
    if (chunk.length === 0) continue;
    const placeholders = chunk.map(() => "?").join(",");
    const result = await testEnv.DB.prepare(
      `SELECT seq, dedup_key, payload_hash, kind, created_at FROM inbox WHERE seq IN (${placeholders})`,
    ).bind(...chunk).all<{
      seq: number;
      dedup_key: string;
      payload_hash: string;
      kind: string;
      created_at: number;
    }>();
    if (!result.success) throw new Error("Unable to build test acknowledgment receipts");
    rows.push(...result.results);
  }
  const bySequence = new Map(rows.map((row) => [row.seq, row]));
  const receipts = seqs.map((seq) => {
    const row = bySequence.get(seq);
    if (row === undefined) throw new Error(`Missing test inbox row for sequence ${seq}`);
    return {
      seq: row.seq,
      request_id: row.dedup_key,
      payload_hash: row.payload_hash,
      kind: row.kind,
      created_at: row.created_at,
    };
  });
  return { receipts };
}

async function signInitData(options: { userId?: number; authDate?: number; chatType?: string; chat?: object } = {}): Promise<string> {
  const params = new URLSearchParams();
  params.set("auth_date", String(options.authDate ?? Math.floor(Date.now() / 1000)));
  params.set("chat_type", options.chatType ?? "private");
  params.set("user", JSON.stringify({ id: options.userId ?? ownerId, is_bot: false, first_name: "Owner" }));
  if (options.chat) params.set("chat", JSON.stringify(options.chat));
  const entries = [...params.entries()].sort(([left], [right]) => left.localeCompare(right));
  const checkString = entries.map(([key, value]) => `${key}=${value}`).join("\n");
  const derivationKey = await crypto.subtle.importKey("raw", textEncoder.encode("WebAppData"), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const secret = await crypto.subtle.sign("HMAC", derivationKey, textEncoder.encode(botToken));
  const verificationKey = await crypto.subtle.importKey("raw", secret, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const digest = new Uint8Array(await crypto.subtle.sign("HMAC", verificationKey, textEncoder.encode(checkString)));
  params.set("hash", [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join(""));
  return params.toString();
}

async function ownerRequest(path: string, initData: string, options: RequestInit = {}): Promise<Response> {
  const headers = new Headers(options.headers ?? {});
  headers.set("x-telegram-init-data", initData);
  if (options.body !== undefined) headers.set("content-type", "application/json");
  return SELF.fetch(new Request(`${workerUrl}${path}`, { ...options, headers }));
}

function localSnapshot(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  const now = Date.now();
  return {
    state_version: 1,
    reported_at: now,
    library: {
      revision: "library-hash-a",
      scanned_at: Math.floor(now / 1000),
      root_label: "Videos",
      items: [],
      categories: [],
      warnings: [],
      ready: true,
    },
    previews: [],
    operations: [],
    tasks: [],
    settings: { download_dir: "/Users/test/Videos" },
    ...overrides,
  };
}

describe("applied media-library snapshot validation", () => {
  it("accepts registered applied settings plus separate per-library availability and snapshots", () => {
    const descriptors = [
      { id: "main", name: "Main", root: "/Volumes/Main", default_category: "Films", is_default: true },
      { id: "archive", name: "Archive", root: "/Volumes/Archive", default_category: "", is_default: false },
    ];
    const snapshot = localSnapshot({
      settings: {
        download_dir: "/Volumes/Main",
        applied: {
          revision: "settings-revision-1",
          default_library_id: "main",
          default_category: "Films",
          libraries: descriptors,
        },
      },
      library: {
        revision: "library-revision-1",
        scanned_at: Math.floor(Date.now() / 1000),
        items: [],
        categories: [],
        warnings: [],
        libraries: [
          { ...descriptors[0], availability: "available", media_count: 0, incomplete_task_count: 0, staging_bytes: 0, snapshot: { items: [], categories: [] } },
          { ...descriptors[1], availability: "offline", media_count: 12, incomplete_task_count: null, staging_bytes: null, snapshot: null },
        ],
        migrations: [],
      },
    });

    expect(validateSnapshot(snapshot).settings.applied?.default_library_id).toBe("main");
  });

  it.each([
    { incomplete_task_count: "0" },
    { incomplete_task_count: -1 },
    { staging_bytes: 1.5 },
    { staging_bytes: Number.MAX_SAFE_INTEGER + 1 },
  ])("rejects invalid per-library task summary fields %#", (summary) => {
    expect(() => validateSnapshot(localSnapshot({
      library: {
        revision: "library-summary-invalid",
        scanned_at: 1,
        items: [],
        categories: [],
        warnings: [],
        libraries: [{
          id: "main",
          name: "Main",
          root: "/Volumes/Main",
          default_category: "",
          availability: "available",
          ...summary,
        }],
      },
    }))).toThrow(/incomplete_task_count|staging_bytes/u);
  });

  it("rejects duplicate or inconsistent default flags in applied library settings", () => {
    const snapshot = localSnapshot({
      settings: {
        download_dir: "/Volumes/Main",
        applied: {
          revision: "settings-revision-1",
          default_library_id: "archive",
          default_category: "",
          libraries: [
            { id: "main", name: "Main", root: "/Volumes/Main", default_category: "", is_default: true },
            { id: "archive", name: "Archive", root: "/Volumes/Archive", default_category: "", is_default: true },
          ],
        },
      },
    });

    expect(() => validateSnapshot(snapshot)).toThrow("exactly one registered default library");
  });

  it("accepts the 32-entry migration projection cap with an omitted-count marker", () => {
    const migrations = Array.from({ length: 32 }, (_, index) => ({ id: `migration-${index}` }));
    const library = {
      revision: "library-revision-cap",
      scanned_at: Math.floor(Date.now() / 1000),
      root_label: "Videos",
      items: [],
      categories: [],
      warnings: [],
      ready: true,
      migrations,
      migration_omitted_count: 1,
    };
    expect(validateSnapshot(localSnapshot({ library })).library.migrations).toHaveLength(32);
    expect(() => validateSnapshot(localSnapshot({
      library: { ...library, migrations: [...migrations, { id: "migration-32" }] },
    }))).toThrow("library.migrations must contain at most 32 entries");
  });
});

function snapshotReadRaceDb(db: D1Database, commitNewSnapshot: () => Promise<void>): D1Database {
  let interleaved = false;
  const wrappedStatements = new WeakMap<object, D1PreparedStatement>();
  const interleaveOnce = async (): Promise<void> => {
    if (interleaved) return;
    interleaved = true;
    await commitNewSnapshot();
  };

  return new Proxy(db, {
    get(target, property) {
      if (property === "prepare") {
        return (query: string) => {
          const statement = target.prepare(query);
          if (!query.includes("FROM snapshot_meta WHERE id = 1")) return statement;

          const wrapped = new Proxy(statement, {
            get(inner, method) {
              if (method === "first") {
                return async (...args: unknown[]) => {
                  const first = inner.first.bind(inner) as (...values: unknown[]) => Promise<unknown>;
                  const result = await first(...args);
                  await interleaveOnce();
                  return result;
                };
              }
              const value = Reflect.get(inner, method, inner) as unknown;
              return typeof value === "function" ? value.bind(inner) : value;
            },
          });
          wrappedStatements.set(wrapped, statement);
          return wrapped;
        };
      }
      if (property === "batch") {
        return async (statements: D1PreparedStatement[]) => {
          await interleaveOnce();
          return target.batch(statements.map((statement) => wrappedStatements.get(statement) ?? statement));
        };
      }
      const value = Reflect.get(target, property, target) as unknown;
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

describe("durable Telegram ingress", () => {
  it("requires the webhook secret and does not wake the notifier for unauthorized traffic", async () => {
    const response = await webhook(ownerMessage(4), "wrong-secret");
    expect(response.status).toBe(401);
    expect(await listDurableObjectIds(testEnv.NOTIFIER)).toHaveLength(0);
    expect((await localRequests()).status).toBe(200);
    expect(await responseJson<{ requests: unknown[] }>(await localRequests())).toMatchObject({ requests: [] });
  });

  it("rejects authorized webhooks until the owner ID is configured", async () => {
    const placeholder = await webhookWithOwnerId(ownerMessage(9), "0");
    const invalid = await webhookWithOwnerId(ownerMessage(10), "not-a-user-id");
    const unauthorized = await webhookWithOwnerId(ownerMessage(11), "0", "wrong-secret");

    expect(placeholder.status).toBe(503);
    expect(invalid.status).toBe(503);
    expect(unauthorized.status).toBe(401);
    expect((await responseJson<{ error: string }>(placeholder)).error).toContain("Owner authorization");
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toHaveLength(0);
    expect(await listDurableObjectIds(testEnv.NOTIFIER)).toHaveLength(0);

    const configured = await webhookWithOwnerId(ownerMessage(12), String(ownerId));
    expect(configured.status).toBe(200);
    expect(await responseJson<{ stored: boolean }>(configured)).toMatchObject({ stored: true });
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toHaveLength(1);
  });

  it("ignores valid updates outside the configured owner's private message/callback path", async () => {
    const nonOwner = await webhook(ownerMessage(5, "/start", ownerId + 1, ownerId + 1));
    const group = await webhook(ownerMessage(6, "/start", ownerId, -100123, "group"));
    const unsupported = await webhook({ update_id: 7, inline_query: { id: "inline", from: { id: ownerId } } });
    const callback = await webhook({ update_id: 8, callback_query: { id: "cb", from: { id: ownerId, is_bot: false }, message: { from: { id: 999, is_bot: true }, chat: { id: ownerId, type: "private" } } } });

    expect(nonOwner.status).toBe(200);
    expect(group.status).toBe(200);
    expect(unsupported.status).toBe(200);
    expect(callback.status).toBe(200);
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toHaveLength(1);
    expect(await listDurableObjectIds(testEnv.NOTIFIER)).toHaveLength(1);
  });

  it("stores the raw owner update before acknowledging and assigns ordered cloud sequences", async () => {
    const first = await webhook(ownerMessage(9000, "/first"));
    const second = await webhook(ownerMessage(2, "/second"));
    expect(first.status).toBe(200);
    expect(second.status).toBe(200);

    const body = await (await localRequests()).json() as { requests: Array<{ seq: number; kind: string; payload: Record<string, unknown> }> };
    expect(body.requests.map((request) => request.seq)).toEqual([1, 2]);
    expect(body.requests.map((request) => request.kind)).toEqual(["telegram", "telegram"]);
    expect((body.requests[0]?.payload.message as { text: string }).text).toBe("/first");
    expect(body.requests[0]?.payload).not.toHaveProperty("update");
  });

  it("deduplicates identical Telegram updates and rejects changed payloads with the same update ID", async () => {
    expect((await webhook(ownerMessage(40, "/same"))).status).toBe(200);
    const duplicate = await webhook(ownerMessage(40, "/same"));
    const conflict = await webhook(ownerMessage(40, "/different"));
    expect(duplicate.status).toBe(200);
    expect((await responseJson<{ duplicate: boolean }>(duplicate)).duplicate).toBe(true);
    expect(conflict.status).toBe(409);
    expect(conflict.headers.get("X-Cloud-State-Version")).toBeNull();
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toHaveLength(1);
  });

  it("returns success only after the D1 insert and never acknowledges a failed insert", async () => {
    const notifierIdsBefore = await listDurableObjectIds(testEnv.NOTIFIER);
    await testEnv.DB.exec("DROP TABLE inbox");
    const response = await webhook(ownerMessage(88));
    expect(response.status).toBe(503);
    expect(await listDurableObjectIds(testEnv.NOTIFIER)).toEqual(notifierIdsBefore);
  });

  it("provides an authenticated WebSocket hint while the D1 backlog remains the reconnect source of truth", async () => {
    const unauthorized = await SELF.fetch(new Request(`${workerUrl}/api/local/ws`, { headers: { upgrade: "websocket" } }));
    expect(unauthorized.status).toBe(401);

    const connected = await SELF.fetch(new Request(`${workerUrl}/api/local/ws`, { headers: { ...localHeaders(), upgrade: "websocket" } }));
    expect(connected.status).toBe(101);
    expect(connected.webSocket).not.toBeNull();

    await webhook(ownerMessage(99, "/backlog"));
    const firstRead = await (await localRequests()).json() as { requests: Array<{ seq: number }> };
    expect(firstRead.requests.map((request) => request.seq)).toEqual([1]);
    const ack = await SELF.fetch(new Request(`${workerUrl}/api/local/ack`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(await acknowledgmentBody([1])),
    }));
    expect(ack.status).toBe(200);
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toEqual([]);
  });
});

describe("cloud database epoch isolation", () => {
  it("bootstraps one epoch and fails closed for stale or missing write scopes", async () => {
    const actualEpoch = await getDatabaseEpoch(testEnv.DB);
    expect(actualEpoch).toBe(testDatabaseEpoch);

    const bootstrap = await SELF.fetch(new Request(workerUrl + "/api/local/requests?limit=1", {
      headers: { authorization: "Bearer " + ownerToken },
    }));
    expect(bootstrap.status).toBe(200);
    expect(await responseJson<{ database_epoch: string; requests: unknown[] }>(bootstrap)).toEqual({
      database_epoch: testDatabaseEpoch,
      requests: [],
    });

    const missingAck = await SELF.fetch(new Request(workerUrl + "/api/local/ack", {
      method: "POST",
      headers: { authorization: "Bearer " + ownerToken, "content-type": "application/json" },
      body: JSON.stringify(await acknowledgmentBody([])),
    }));
    const missingState = await SELF.fetch(new Request(workerUrl + "/api/local/state", {
      method: "POST",
      headers: { authorization: "Bearer " + ownerToken, "content-type": "application/json" },
      body: JSON.stringify(localSnapshot()),
    }));
    expect(missingAck.status).toBe(428);
    expect(missingState.status).toBe(428);

    const staleEpoch = "ffffffffffffffffffffffffffffffff";
    const stalePage = await SELF.fetch(new Request(workerUrl + "/api/local/requests", {
      headers: { ...localHeaders(), "X-Cloud-Database-Epoch": staleEpoch },
    }));
    const staleAck = await SELF.fetch(new Request(workerUrl + "/api/local/ack", {
      method: "POST",
      headers: { ...localHeaders(), "X-Cloud-Database-Epoch": staleEpoch, "content-type": "application/json" },
      body: JSON.stringify(await acknowledgmentBody([])),
    }));
    const staleState = await SELF.fetch(new Request(workerUrl + "/api/local/state", {
      method: "POST",
      headers: { ...localHeaders(), "X-Cloud-Database-Epoch": staleEpoch, "content-type": "application/json" },
      body: JSON.stringify(localSnapshot()),
    }));
    const staleSocket = await SELF.fetch(new Request(workerUrl + "/api/local/ws", {
      headers: { ...localHeaders(), "X-Cloud-Database-Epoch": staleEpoch, upgrade: "websocket" },
    }));
    for (const response of [stalePage, staleAck, staleState, staleSocket]) {
      expect(response.status).toBe(409);
      expect(response.headers.get("X-Cloud-Database-Epoch")).toBe(testDatabaseEpoch);
    }
    expect(await getDatabaseEpoch(testEnv.DB)).toBe(testDatabaseEpoch);
  });

  it("rotates only for an exact pending sequence collision and retires the prior snapshot", async () => {
    const stored = await enqueue(testEnv.DB, "action:epoch-reset-case", "library_scan", { root: "/Videos" });
    expect(stored.envelope.seq).toBe(1);
    const snapshotResponse = await SELF.fetch(new Request(workerUrl + "/api/local/state", {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(localSnapshot({ state_version: 14 })),
    }));
    expect(snapshotResponse.status).toBe(200);
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM snapshot_meta").first<{ count: number }>())?.count).toBe(1);

    const rotationUrl = workerUrl + "/api/local/epoch/rotate-on-conflict";
    const wrongEnvelope = await SELF.fetch(new Request(rotationUrl, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({
        seq: stored.envelope.seq,
        request_id: stored.envelope.request_id,
        payload_hash: "0".repeat(64),
        kind: stored.envelope.kind,
        created_at: stored.envelope.created_at,
      }),
    }));
    expect(wrongEnvelope.status).toBe(409);
    expect(wrongEnvelope.headers.get("X-Cloud-Database-Epoch")).toBe(testDatabaseEpoch);
    expect(await getDatabaseEpoch(testEnv.DB)).toBe(testDatabaseEpoch);

    const rotated = await SELF.fetch(new Request(rotationUrl, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({
        seq: stored.envelope.seq,
        request_id: stored.envelope.request_id,
        payload_hash: stored.envelope.payload_hash,
        kind: stored.envelope.kind,
        created_at: stored.envelope.created_at,
      }),
    }));
    expect(rotated.status).toBe(200);
    const rotation = await responseJson<{ rotated: boolean; database_epoch: string }>(rotated);
    expect(rotation.rotated).toBe(true);
    expect(rotation.database_epoch).toMatch(/^[0-9a-f]{32}$/u);
    expect(rotation.database_epoch).not.toBe(testDatabaseEpoch);
    expect(await getDatabaseEpoch(testEnv.DB)).toBe(rotation.database_epoch);
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM snapshot_meta").first<{ count: number }>())?.count).toBe(0);
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM snapshot_chunks").first<{ count: number }>())?.count).toBe(0);

    const staleAck = await SELF.fetch(new Request(workerUrl + "/api/local/ack", {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(await acknowledgmentBody([stored.envelope.seq])),
    }));
    expect(staleAck.status).toBe(409);
    expect(staleAck.headers.get("X-Cloud-Database-Epoch")).toBe(rotation.database_epoch);
    expect((await testEnv.DB.prepare("SELECT acked_at FROM inbox WHERE seq = 1").first<{ acked_at: number | null }>())?.acked_at)
      .toBeNull();

    const stalePage = await SELF.fetch(new Request(workerUrl + "/api/local/requests", { headers: localHeaders() }));
    expect(stalePage.status).toBe(409);
    expect(stalePage.headers.get("X-Cloud-Database-Epoch")).toBe(rotation.database_epoch);
    const reboundPage = await SELF.fetch(new Request(workerUrl + "/api/local/requests", {
      headers: { ...localHeaders(), "X-Cloud-Database-Epoch": rotation.database_epoch },
    }));
    expect(reboundPage.status).toBe(200);
    const page = await responseJson<{
      database_epoch: string;
      requests: Array<{ seq: number; kind: string; payload: Record<string, unknown>; created_at: number; request_id: string; payload_hash: string }>;
    }>(reboundPage);
    expect(page.database_epoch).toBe(rotation.database_epoch);
    expect(page.requests).toEqual([{
      seq: stored.envelope.seq,
      kind: stored.envelope.kind,
      payload: { root: "/Videos" },
      created_at: stored.envelope.created_at,
      request_id: stored.envelope.request_id,
      payload_hash: stored.envelope.payload_hash,
    }]);

    const reboundAck = await SELF.fetch(new Request(workerUrl + "/api/local/ack", {
      method: "POST",
      headers: { ...localHeaders(), "X-Cloud-Database-Epoch": rotation.database_epoch, "content-type": "application/json" },
      body: JSON.stringify(await acknowledgmentBody([stored.envelope.seq])),
    }));
    expect(reboundAck.status).toBe(200);
    expect(await responseJson<{ acked: number }>(reboundAck)).toEqual({ acked: 1 });
  });

  it("rejects an entire ACK batch when a reused sequence has a different envelope", async () => {
    const oldRequest = await enqueue(testEnv.DB, "action:before-database-rewind", "library_scan", { root: "/Old" });
    const nextRequest = await enqueue(testEnv.DB, "action:unrelated-page-entry", "library_scan", { root: "/Other" });
    expect(oldRequest.envelope.seq).toBe(1);
    expect(nextRequest.envelope.seq).toBe(2);

    await testEnv.DB.prepare(
      "UPDATE inbox SET dedup_key = ?, payload_hash = ?, payload_json = ?, created_at = ? WHERE seq = ?",
    ).bind(
      "action:after-database-rewind",
      "c".repeat(64),
      JSON.stringify({ root: "/Restored" }),
      oldRequest.envelope.created_at + 1,
      oldRequest.envelope.seq,
    ).run();

    const rejected = await SELF.fetch(new Request(workerUrl + "/api/local/ack", {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({ receipts: [oldRequest.envelope, nextRequest.envelope] }),
    }));
    expect(rejected.status).toBe(409);
    expect(rejected.headers.get("X-Cloud-Inbox-Sequence-Conflict")).toBe("1");
    expect((await testEnv.DB.prepare("SELECT acked_at FROM inbox WHERE seq IN (1, 2) ORDER BY seq").all<{ acked_at: number | null }>()).results)
      .toEqual([{ acked_at: null }, { acked_at: null }]);

    const currentReceipts = await acknowledgmentBody([1, 2]);
    const accepted = await SELF.fetch(new Request(workerUrl + "/api/local/ack", {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(currentReceipts),
    }));
    expect(accepted.status).toBe(200);
    expect(await responseJson<{ acked: number }>(accepted)).toEqual({ acked: 2 });
  });
});

describe("Mini App identity and actions", () => {
  it("verifies signed Telegram initData, owner, private chat, freshness, and tamper resistance", async () => {
    const valid = await ownerRequest("/api/state", await signInitData());
    expect(valid.status).toBe(200);
    expect((await responseJson<{ synced: boolean }>(valid)).synced).toBe(false);

    const wrongOwner = await ownerRequest("/api/state", await signInitData({ userId: ownerId + 1 }));
    const group = await ownerRequest("/api/state", await signInitData({ chatType: "group" }));
    const stale = await ownerRequest("/api/state", await signInitData({ authDate: Math.floor(Date.now() / 1000) - 7 * 60 * 60 }));
    const signed = new URLSearchParams(await signInitData());
    signed.set("user", JSON.stringify({ id: ownerId + 1, is_bot: false }));
    const tampered = await ownerRequest("/api/state", signed.toString());
    expect(wrongOwner.status).toBe(403);
    expect(group.status).toBe(403);
    expect(stale.status).toBe(401);
    expect(tampered.status).toBe(401);
  });

  it("enqueues an idempotent validated action and detects request ID reuse across payload or kind", async () => {
    const initData = await signInitData();
    const action = {
      request_id: "00000000-0000-4000-8000-000000000001",
      kind: "file_preview",
      payload: { item_ids: ["item-a"], target_relative_dir: "", rename: false, conflict: "keep_both" },
    };
    const first = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify(action) });
    const duplicate = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify(action) });
    const changed = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...action, kind: "library_scan", payload: {} }) });
    expect(first.status).toBe(202);
    expect(duplicate.status).toBe(202);
    expect((await responseJson<{ duplicate: boolean }>(duplicate)).duplicate).toBe(true);
    expect(changed.status).toBe(409);
  });

  it("persists validated multi-library settings, downloads, and migration actions through the inbox", async () => {
    const initData = await signInitData();
    const actions = [
      { kind: "library_register", payload: { name: "Archive", path: "/Volumes/Archive", default_category: "Films", set_default: false, create_directory: false } },
      { kind: "library_set_default", payload: { library_id: "library-archive" } },
      { kind: "library_set_category", payload: { library_id: "library-archive", default_category: "Films/Recent" } },
      { kind: "library_rename", payload: { library_id: "library-archive", name: "Film archive" } },
      { kind: "library_remove", payload: { library_id: "library-archive" } },
      { kind: "library_scan", payload: { library_id: "library-archive" } },
      { kind: "file_preview", payload: { library_id: "library-archive", item_ids: ["item-a"], target_relative_dir: "Films" } },
      { kind: "file_confirm", payload: { preview_id: "preview-a", revision: "revision-a", library_id: "library-archive" } },
      { kind: "legacy_import", payload: { text: "older links", library_id: "library-archive" } },
      { kind: "download_submit", payload: { url: "https://www.bilibili.com/video/BV1xx", library_id: "library-archive", category: "Films" } },
      { kind: "download_submit", payload: { url: "https://www.bilibili.com/video/BV1omitted", library_id: "library-archive" } },
      { kind: "download_submit", payload: { url: "https://www.bilibili.com/video/BV1root", library_id: "library-archive", category: "" } },
      { kind: "library_migration_preview", payload: { source_library_id: "library-main", target_library_id: "library-archive", scope: "library", include_incomplete_tasks: false, conflict: "skip", item_conflicts: { "item-a": "keep_both" }, set_target_default_on_success: false, supersedes_preview_seq: 19, supersedes_preview_id: "migration-preview-old", supersedes_revision: "revision-old" } },
      { kind: "library_migration_confirm", payload: { preview_id: "migration-preview-a", revision: "revision-a" } },
      { kind: "library_migration_control", payload: { migration_id: "migration-a", control: "resume" } },
    ];

    for (const [index, action] of actions.entries()) {
      const response = await ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: `00000000-0000-4000-8000-${String(0x9400 + index).padStart(12, "0")}`,
          ...action,
        }),
      });
      expect(response.status, action.kind).toBe(202);
    }

    const backlog = await responseJson<{ requests: Array<{ kind: string; payload: Record<string, unknown> }> }>(await localRequests());
    expect(backlog.requests.map((request) => request.kind)).toEqual(actions.map((action) => action.kind));
    expect(backlog.requests[9]?.payload).toMatchObject({ library_id: "library-archive", category: "Films" });
    expect(backlog.requests[10]?.payload).toMatchObject({ library_id: "library-archive" });
    expect(backlog.requests[10]?.payload).not.toHaveProperty("category");
    expect(backlog.requests[11]?.payload).toMatchObject({ library_id: "library-archive", category: "" });
    expect(backlog.requests[12]?.payload).toMatchObject({
      scope: "library",
      conflict: "skip",
      item_conflicts: { "item-a": "keep_both" },
      include_incomplete_tasks: false,
      supersedes_preview_seq: 19,
      supersedes_preview_id: "migration-preview-old",
      supersedes_revision: "revision-old",
    });
  });

  it("rejects unsafe media-library roots, URLs, migration selections, and controls before persistence", async () => {
    const initData = await signInitData();
    const cases = [
      { kind: "library_register", payload: { name: "Relative", path: "Videos", default_category: "", set_default: false, create_directory: false } },
      { kind: "download_submit", payload: { url: "file:///etc/passwd", library_id: "library-a", category: "" } },
      { kind: "download_submit", payload: { url: "https://example.com/video", library_id: "library-a", category: "../outside" } },
      { kind: "library_migration_preview", payload: { source_library_id: "library-a", target_library_id: "library-a", scope: "library" } },
      { kind: "library_migration_preview", payload: { source_library_id: "library-a", target_library_id: "library-b", scope: "selected", item_ids: [], include_incomplete_tasks: false } },
      { kind: "library_migration_preview", payload: { source_library_id: "library-a", target_library_id: "library-b", scope: "selected", item_ids: ["item-a"], item_conflicts: { "item-b": "keep_both" } } },
      { kind: "library_migration_preview", payload: { source_library_id: "library-a", target_library_id: "library-b", scope: "library", item_conflicts: { "item-a": "overwrite" } } },
      { kind: "library_migration_preview", payload: { source_library_id: "library-a", target_library_id: "library-b", scope: "library", item_conflicts: Object.fromEntries(Array.from({ length: WORKER_MAX_MIGRATION_ITEM_CONFLICTS + 1 }, (_, index) => [`item-${index}`, "skip"])) } },
      { kind: "library_migration_preview", payload: { source_library_id: "library-a", target_library_id: "library-b", scope: "library", supersedes_preview_seq: 5 } },
      { kind: "library_migration_preview", payload: { source_library_id: "library-a", target_library_id: "library-b", scope: "library", supersedes_preview_seq: 0, supersedes_preview_id: "preview-a", supersedes_revision: "revision-a" } },
      { kind: "library_migration_preview", payload: { source_library_id: "library-a", target_library_id: "library-b", scope: "library", supersedes_preview_seq: 5, supersedes_preview_id: "preview-a", supersedes_revision: "  " } },
      { kind: "library_migration_control", payload: { migration_id: "migration-a", control: "delete_all" } },
    ];

    for (const [index, action] of cases.entries()) {
      const response = await ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: `00000000-0000-4000-8000-${String(0x9500 + index).padStart(12, "0")}`,
          ...action,
        }),
      });
      expect(response.status, action.kind).toBe(400);
    }
    expect((await localRequests()).status).toBe(200);
    expect((await responseJson<{ requests: unknown[] }>(await localRequests())).requests).toHaveLength(0);
  });

  it("shows a cloud-accepted request while the desktop is offline without exposing its payload", async () => {
    const initData = await signInitData();
    const accepted = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000000010",
        kind: "file_preview",
        payload: { item_ids: ["private-item"], target_relative_dir: "Private/Target" },
      }),
    });
    expect(accepted.status).toBe(202);

    const state = await responseJson<{ synced: boolean; pending_requests: Array<Record<string, unknown>> }>(await ownerRequest("/api/state", initData));
    expect(state.synced).toBe(false);
    expect(state.pending_requests).toEqual([
      { seq: 1, kind: "file_preview", created_at: expect.any(Number), status: "waiting_for_local" },
    ]);
    expect(state.pending_requests[0]).not.toHaveProperty("payload");

    const desktopBacklog = await responseJson<{ requests: Array<{ payload: { item_ids: string[] } }> }>(await localRequests());
    expect(desktopBacklog.requests[0]?.payload.item_ids).toEqual(["private-item"]);
  });

  it("preserves legacy line separators while enforcing decoded UTF-8 text bounds before enqueueing", async () => {
    const initData = await signInitData();
    const separatorText = "Line 1\r\n\tLine 2\n";
    const exactUtf8Limit = `${"界".repeat(87_381)}x`;
    const oversizedUtf8 = `${exactUtf8Limit}x`;
    expect(textEncoder.encode(exactUtf8Limit).byteLength).toBe(MAX_LEGACY_TEXT_BYTES);
    expect(textEncoder.encode(oversizedUtf8).byteLength).toBe(MAX_LEGACY_TEXT_BYTES + 1);

    const cases = [
      { requestId: "00000000-0000-4000-8000-000000009302", text: separatorText, expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000009303", text: exactUtf8Limit, expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000009304", text: oversizedUtf8, expectedStatus: 413 },
      { requestId: "00000000-0000-4000-8000-000000009305", text: "Unsafe\u0001control", expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000009306", text: "Unsafe\u007fcontrol", expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000009307", text: "Unsafe\u0085control", expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000009308", text: "Malformed\uD800text", expectedStatus: 400 },
    ];
    const before = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();

    for (const testCase of cases) {
      const response = await ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: testCase.requestId,
          kind: "legacy_import",
          payload: { text: testCase.text },
        }),
      });
      expect(response.status, testCase.requestId).toBe(testCase.expectedStatus);
    }

    const after = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    expect(after?.count).toBe((before?.count ?? 0) + 2);
    const backlog = await responseJson<{ requests: Array<{ kind: string; payload: { text?: string } }> }>(await localRequests());
    expect(backlog.requests.some((request) => request.kind === "legacy_import" && request.payload.text === separatorText)).toBe(true);
    expect(backlog.requests.some((request) => request.kind === "legacy_import" && request.payload.text === exactUtf8Limit)).toBe(true);
  });

  it("accepts near-two-times JSON.stringify legacy bodies and bounds excess JSON whitespace", async () => {
    const initData = await signInitData();
    const lineText = "\n".repeat(MAX_LEGACY_TEXT_BYTES);
    const acceptedBody = JSON.stringify({
      request_id: "00000000-0000-4000-8000-000000009309",
      kind: "legacy_import",
      payload: { text: lineText },
    });
    const acceptedBytes = textEncoder.encode(acceptedBody).byteLength;
    expect(acceptedBytes).toBeGreaterThan(320_000);
    expect(acceptedBytes).toBeLessThanOrEqual(MAX_ACTION_BODY_BYTES);

    const paddingBase = JSON.stringify({
      request_id: "00000000-0000-4000-8000-000000009310",
      kind: "library_scan",
      payload: {},
    });
    const oversizedBody = `${paddingBase}${" ".repeat(MAX_ACTION_BODY_BYTES - textEncoder.encode(paddingBase).byteLength + 1)}`;
    expect(textEncoder.encode(oversizedBody).byteLength).toBe(MAX_ACTION_BODY_BYTES + 1);
    const before = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();

    const accepted = await ownerRequest("/api/actions", initData, { method: "POST", body: acceptedBody });
    const oversized = await ownerRequest("/api/actions", initData, { method: "POST", body: oversizedBody });
    expect(accepted.status).toBe(202);
    expect(oversized.status).toBe(413);

    const after = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    expect(after?.count).toBe((before?.count ?? 0) + 1);
  });

  it("allows each legacy hint to appear in only one metadata patch", async () => {
    const initData = await signInitData();
    const before = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    const duplicateHint = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000009311",
        kind: "file_preview",
        payload: {
          metadata_patches: [
            { item_id: "item-a", hint_index: 7, source_ids: ["source-a"] },
            { item_id: "item-b", hint_index: 7, source_ids: ["source-b"] },
          ],
        },
      }),
    });
    const noHints = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000009312",
        kind: "file_preview",
        payload: {
          metadata_patches: [
            { item_id: "item-a", title: "Manual A" },
            { item_id: "item-b", title: "Manual B" },
          ],
        },
      }),
    });
    const oneHint = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000009313",
        kind: "file_preview",
        payload: {
          metadata_patches: [
            { item_id: "item-a", hint_index: 7, source_ids: ["source-a"] },
            { item_id: "item-b", title: "Manual B" },
          ],
        },
      }),
    });
    expect(duplicateHint.status).toBe(400);
    expect(noHints.status).toBe(202);
    expect(oneHint.status).toBe(202);

    const after = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    expect(after?.count).toBe((before?.count ?? 0) + 2);
  });

  it("rejects unsafe destinations and malformed or oversized metadata patches", async () => {
    const initData = await signInitData();
    const base = { request_id: "00000000-0000-4000-8000-000000000002", kind: "file_preview", payload: {} };
    const unsafe = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, payload: { item_ids: ["item-a"], target_relative_dir: "../private" } }) });
    const invalidPatch = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, payload: { metadata_patches: [{ item_id: "item-a", hint_index: 10000, source_ids: ["source-a"] }] } }) });
    const maxItems = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, request_id: "00000000-0000-4000-8000-000000000022", payload: { item_ids: Array.from({ length: MAX_SELECTED_ITEMS }, (_, index) => `item-${index}`), target_relative_dir: "" } }) });
    const maxPatches = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, request_id: "00000000-0000-4000-8000-000000000023", payload: { metadata_patches: Array.from({ length: MAX_SELECTED_ITEMS }, (_, index) => ({ item_id: `item-${index}`, title: "Selected" })) } }) });
    const tooManyItems = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, request_id: "00000000-0000-4000-8000-000000000020", payload: { item_ids: Array.from({ length: MAX_SELECTED_ITEMS + 1 }, (_, index) => `item-${index}`) } }) });
    const tooManyPatches = await ownerRequest("/api/actions", initData, { method: "POST", body: JSON.stringify({ ...base, request_id: "00000000-0000-4000-8000-000000000021", payload: { metadata_patches: Array.from({ length: MAX_SELECTED_ITEMS + 1 }, (_, index) => ({ item_id: `item-${index}`, title: "Selected" })) } }) });
    expect(unsafe.status).toBe(400);
    expect(invalidPatch.status).toBe(400);
    expect(maxItems.status).toBe(202);
    expect(maxPatches.status).toBe(202);
    expect(tooManyItems.status).toBe(400);
    expect(tooManyPatches.status).toBe(400);
  });

  it("accepts only target folders within both UTF-8 path limits before enqueueing", async () => {
    const initData = await signInitData();
    const cjkSegment = "界".repeat(70);
    const exact = [cjkSegment, cjkSegment, cjkSegment, cjkSegment, "x".repeat(180)].join("/");
    const oversized = [cjkSegment, cjkSegment, cjkSegment, cjkSegment, "x".repeat(181)].join("/");
    const sixLevelSeventyCjk = Array.from({ length: 6 }, () => cjkSegment).join("/");
    const acceptedComponents = [
      "x".repeat(255),
      "界".repeat(85),
      `${"😀".repeat(63)}abc`,
    ];
    const rejectedComponents = [
      "x".repeat(256),
      `${"界".repeat(85)}a`,
      `${"😀".repeat(63)}abcd`,
    ];
    const maxComponentsPath = Array.from({ length: 4 }, () => "x".repeat(255)).join("/");
    expect(textEncoder.encode(exact).byteLength).toBe(1_024);
    expect(textEncoder.encode(oversized).byteLength).toBe(1_025);
    expect(textEncoder.encode(sixLevelSeventyCjk).byteLength).toBe(1_265);
    expect(CLIENT_MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES).toBe(
      WORKER_MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES,
    );
    expect(acceptedComponents.map((segment) => textEncoder.encode(segment).byteLength)).toEqual([255, 255, 255]);
    expect(rejectedComponents.map((segment) => textEncoder.encode(segment).byteLength)).toEqual([256, 256, 256]);
    expect(textEncoder.encode(maxComponentsPath).byteLength).toBe(1_023);

    const invalidPaths = [
      oversized,
      sixLevelSeventyCjk,
      ...rejectedComponents,
      "Movies/Part\nTwo",
      "Movies/\uD800",
      "Movies/.TELEGRAM-VIDEO-DOWNLOADER-LIBRARY/cache",
    ];
    const before = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    const notifierIdsBefore = await listDurableObjectIds(testEnv.NOTIFIER);
    for (const [index, targetRelativeDir] of invalidPaths.entries()) {
      const response = await ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: `00000000-0000-4000-8000-${String(930 + index).padStart(12, "0")}`,
          kind: "file_preview",
          payload: { item_ids: ["item-a"], target_relative_dir: targetRelativeDir },
        }),
      });
      expect(response.status, `target path ${index}`).toBe(400);
    }
    const afterInvalid = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    expect(afterInvalid?.count).toBe(before?.count);
    expect(await listDurableObjectIds(testEnv.NOTIFIER)).toEqual(notifierIdsBefore);

    const rootAccepted = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000000940",
        kind: "file_preview",
        payload: { item_ids: ["item-a"], target_relative_dir: "" },
      }),
    });
    const exactAccepted = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000000941",
        kind: "file_preview",
        payload: { item_ids: ["item-a"], target_relative_dir: exact },
      }),
    });
    const componentAccepted = await Promise.all(acceptedComponents.map((targetRelativeDir, index) =>
      ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: `00000000-0000-4000-8000-${String(942 + index).padStart(12, "0")}`,
          kind: "file_preview",
          payload: { item_ids: ["item-a"], target_relative_dir: targetRelativeDir },
        }),
      }),
    ));
    const maxPathAccepted = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000000945",
        kind: "file_preview",
        payload: { item_ids: ["item-a"], target_relative_dir: maxComponentsPath },
      }),
    });
    expect(rootAccepted.status).toBe(202);
    expect(exactAccepted.status).toBe(202);
    expect(componentAccepted.map((response) => response.status)).toEqual([202, 202, 202]);
    expect(maxPathAccepted.status).toBe(202);
    const afterAccepted = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    expect(afterAccepted?.count).toBe((before?.count ?? 0) + 6);
  });

  it("rejects case-insensitive and HFS-ignorable private directory aliases before enqueueing", async () => {
    const initData = await signInitData();
    const aliases = [
      ".TELEGRAM-VIDEO-DOWNLOADER-LIBRARY",
      "nested/.TeLeGrAm-ViDeO-DoWnLoAdEr-LiBrArY-cache",
      ".telegram-video-down\u200cloader-library-cache",
      "nested/.telegram-video-down\u200floader-staging",
      ".telegram-video-down\u202aloader-state",
      "nested/.telegram-video-down\u202eloader-private",
      ".telegram-video-down\u206aloader-staging",
      "nested/.telegram-video-down\u206floader-private",
      ".telegram-video-down\ufeffloader-library",
      ".TELEGRAM-VIDEO-DOWNLOADER-STATE",
      "nested/.tElEgRaM-vIdEo-dOwNlOaDeR-staging",
    ];
    const beforeRows = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    const beforeNotifierIds = await listDurableObjectIds(testEnv.NOTIFIER);

    for (const [index, targetRelativeDir] of aliases.entries()) {
      const response = await ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: `00000000-0000-4000-8000-${String(3001 + index).padStart(12, "0")}`,
          kind: "file_preview",
          payload: { item_ids: ["item-a"], target_relative_dir: targetRelativeDir },
        }),
      });
      expect(response.status).toBe(400);
    }

    const afterRejectedRows = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    expect(afterRejectedRows?.count).toBe(beforeRows?.count);
    expect(await listDurableObjectIds(testEnv.NOTIFIER)).toEqual(beforeNotifierIds);

    const accepted = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-00000000030f",
        kind: "file_preview",
        payload: { item_ids: ["item-a"], target_relative_dir: "Normal/With\u200cIgnorable" },
      }),
    });
    expect(accepted.status).toBe(202);

    const afterAcceptedRows = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    expect(afterAcceptedRows?.count).toBe((beforeRows?.count ?? 0) + 1);
  });

  it("bounds collection order to Rust u32 before enqueueing", async () => {
    const initData = await signInitData();
    const orderCases: Array<{ requestId: string; order: unknown; expectedStatus: number }> = [
      { requestId: "00000000-0000-4000-8000-000000000201", order: 0, expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000202", order: 4_294_967_295, expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000203", order: 4_294_967_296, expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000000204", order: Number.MAX_SAFE_INTEGER, expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000000205", order: 1.5, expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000000206", order: -1, expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000000207", order: "1", expectedStatus: 400 },
    ];
    const before = await responseJson<{ requests: unknown[] }>(await localRequests());

    for (const orderCase of orderCases) {
      const response = await ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: orderCase.requestId,
          kind: "file_preview",
          payload: {
            metadata_patches: [{
              item_id: "item-a",
              collection: { id: null, title: "Series", kind: "series", order: orderCase.order, part_id: null },
            }],
          },
        }),
      });
      expect(response.status, orderCase.requestId).toBe(orderCase.expectedStatus);
    }

    const after = await responseJson<{ requests: unknown[] }>(await localRequests());
    expect(after.requests).toHaveLength(before.requests.length + 2);
  });

  it("validates collection identifiers against the UTF-8 byte limit before enqueueing", async () => {
    const initData = await signInitData();
    const cjk128Bytes = `${"界".repeat(42)}ab`;
    const cjk129Bytes = `${"界".repeat(42)}abc`;
    const emoji128Bytes = "😀".repeat(32);
    const emoji132Bytes = "😀".repeat(33);
    const values = [
      { label: "ASCII 128 bytes", value: "a".repeat(128), expectedStatus: 202 },
      { label: "ASCII 129 bytes", value: "a".repeat(129), expectedStatus: 400 },
      { label: "CJK 128 bytes", value: cjk128Bytes, expectedStatus: 202 },
      { label: "CJK 129 bytes", value: cjk129Bytes, expectedStatus: 400 },
      { label: "emoji 128 bytes", value: emoji128Bytes, expectedStatus: 202 },
      { label: "emoji 132 bytes", value: emoji132Bytes, expectedStatus: 400 },
      { label: "unpaired surrogate", value: "\uD800", expectedStatus: 400 },
    ];
    expect(textEncoder.encode(cjk128Bytes).byteLength).toBe(128);
    expect(textEncoder.encode(cjk129Bytes).byteLength).toBe(129);
    expect(textEncoder.encode(emoji128Bytes).byteLength).toBe(128);
    expect(textEncoder.encode(emoji132Bytes).byteLength).toBe(132);
    const identifierCases = (["id", "part_id"] as const).flatMap((field, fieldIndex) =>
      values.map((valueCase, valueIndex) => ({
        field,
        ...valueCase,
        requestId: `00000000-0000-4000-8000-${String(700 + fieldIndex * values.length + valueIndex).padStart(12, "0")}`,
      })));
    const before = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();

    for (const identifierCase of identifierCases) {
      const collection: Record<string, unknown> = {
        id: null,
        title: "Series",
        kind: "series",
        order: null,
        part_id: null,
      };
      collection[identifierCase.field] = identifierCase.value;
      const response = await ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: identifierCase.requestId,
          kind: "file_preview",
          payload: {
            metadata_patches: [{
              item_id: "item-a",
              collection,
            }],
          },
        }),
      });
      expect(response.status, `${identifierCase.field}: ${identifierCase.label}`).toBe(identifierCase.expectedStatus);
    }

    const after = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    expect(after?.count).toBe((before?.count ?? 0) + 6);
  });

  it("validates collection kinds against the UTF-8 byte limit before enqueueing", async () => {
    const initData = await signInitData();
    const cjk64Bytes = `${"界".repeat(21)}a`;
    const cjk65Bytes = `${"界".repeat(21)}ab`;
    const emoji64Bytes = "😀".repeat(16);
    const emoji68Bytes = "😀".repeat(17);
    const kindCases = [
      { label: "ASCII 64 bytes", value: "a".repeat(64), expectedStatus: 202 },
      { label: "ASCII 65 bytes", value: "a".repeat(65), expectedStatus: 400 },
      { label: "CJK 64 bytes", value: cjk64Bytes, expectedStatus: 202 },
      { label: "CJK 65 bytes", value: cjk65Bytes, expectedStatus: 400 },
      { label: "emoji 64 bytes", value: emoji64Bytes, expectedStatus: 202 },
      { label: "emoji 68 bytes", value: emoji68Bytes, expectedStatus: 400 },
      { label: "unpaired surrogate", value: "\uD800", expectedStatus: 400 },
    ];
    expect(textEncoder.encode(cjk64Bytes).byteLength).toBe(64);
    expect(textEncoder.encode(cjk65Bytes).byteLength).toBe(65);
    expect(textEncoder.encode(emoji64Bytes).byteLength).toBe(64);
    expect(textEncoder.encode(emoji68Bytes).byteLength).toBe(68);
    const before = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();

    for (const [index, kindCase] of kindCases.entries()) {
      const response = await ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: `00000000-0000-4000-8000-${String(800 + index).padStart(12, "0")}`,
          kind: "file_preview",
          payload: {
            metadata_patches: [{
              item_id: "item-a",
              collection: { id: null, title: "Series", kind: kindCase.value, order: null, part_id: null },
            }],
          },
        }),
      });
      expect(response.status, kindCase.label).toBe(kindCase.expectedStatus);
    }

    const after = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    expect(after?.count).toBe((before?.count ?? 0) + 3);
  });

  it("accepts only hint indices in the bounded legacy hint range before enqueueing", async () => {
    const initData = await signInitData();
    const hintCases: Array<{ requestId: string; hintIndex: number; expectedStatus: number }> = [
      { requestId: "00000000-0000-4000-8000-000000000601", hintIndex: 0, expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000602", hintIndex: 999, expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000603", hintIndex: 1000, expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000604", hintIndex: 9999, expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000605", hintIndex: 10000, expectedStatus: 400 },
    ];
    const before = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();

    for (const hintCase of hintCases) {
      const response = await ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: hintCase.requestId,
          kind: "file_preview",
          payload: {
            metadata_patches: [{ item_id: "item-a", hint_index: hintCase.hintIndex, source_ids: ["source-a"] }],
          },
        }),
      });
      expect(response.status, hintCase.requestId).toBe(hintCase.expectedStatus);
    }

    const after = await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox").first<{ count: number }>();
    expect(after?.count).toBe((before?.count ?? 0) + 4);
  });

  it("validates metadata titles by Unicode scalar count before enqueueing", async () => {
    const initData = await signInitData();
    const cjkWith501Utf8Bytes = "界".repeat(167);
    expect(textEncoder.encode(cjkWith501Utf8Bytes).byteLength).toBe(501);

    const titleCases: Array<{
      requestId: string;
      title: string;
      expectedStatus: number;
      collection?: boolean;
    }> = [
      { requestId: "00000000-0000-4000-8000-000000000101", title: "A".repeat(500), expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000102", title: "A".repeat(501), expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000000103", title: "界".repeat(500), expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000104", title: "界".repeat(501), expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000000105", title: cjkWith501Utf8Bytes, expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000106", title: "😀".repeat(500), expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000107", title: "😀".repeat(501), expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000000108", title: ` ${"A".repeat(500)} `, expectedStatus: 202 },
      { requestId: "00000000-0000-4000-8000-000000000109", title: "😀".repeat(500), expectedStatus: 202, collection: true },
      { requestId: "00000000-0000-4000-8000-000000000110", title: "😀".repeat(501), expectedStatus: 400, collection: true },
      { requestId: "00000000-0000-4000-8000-000000000111", title: "\uD800", expectedStatus: 400 },
      { requestId: "00000000-0000-4000-8000-000000000112", title: "Invalid\u0001Title", expectedStatus: 400 },
    ];
    const before = await responseJson<{ requests: unknown[] }>(await localRequests());

    for (const titleCase of titleCases) {
      const metadataPatch = titleCase.collection
        ? {
            item_id: "item-a",
            collection: { id: null, title: titleCase.title, kind: "series", order: null, part_id: null },
          }
        : { item_id: "item-a", title: titleCase.title };
      const response = await ownerRequest("/api/actions", initData, {
        method: "POST",
        body: JSON.stringify({
          request_id: titleCase.requestId,
          kind: "file_preview",
          payload: { metadata_patches: [metadataPatch] },
        }),
      });
      expect(response.status, titleCase.requestId).toBe(titleCase.expectedStatus);
    }

    const after = await responseJson<{ requests: unknown[] }>(await localRequests());
    const accepted = titleCases.filter((titleCase) => titleCase.expectedStatus === 202).length;
    expect(after.requests).toHaveLength(before.requests.length + accepted);
  });

  it("accepts a manual metadata patch without a hint index and keeps metadata-only items exact", async () => {
    const initData = await signInitData();
    const accepted = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000000003",
        kind: "file_preview",
        payload: { metadata_patches: [{ item_id: "item-a", title: "Selected manually", source_ids: ["source-a"] }] },
      }),
    });
    const extraItem = await ownerRequest("/api/actions", initData, {
      method: "POST",
      body: JSON.stringify({
        request_id: "00000000-0000-4000-8000-000000000004",
        kind: "file_preview",
        payload: { item_ids: ["item-a", "item-b"], metadata_patches: [{ item_id: "item-a", title: "Selected manually" }] },
      }),
    });
    expect(accepted.status).toBe(202);
    expect(extraItem.status).toBe(400);
  });
});

describe("local client API and state snapshots", () => {
  it.each([
    { label: "same", paddingBytes: 0 },
    { label: "different", paddingBytes: 500_100 },
  ])("reads a consistent snapshot through both owner APIs when a write lands between queries ($label chunk counts)", async ({ paddingBytes }) => {
    const owner = await signInitData();
    for (const path of ["/api/state", "/api/library"] as const) {
      const current = await testEnv.DB.prepare(
        "SELECT state_version, reported_at FROM snapshot_meta WHERE id = 1",
      ).first<{ state_version: number; reported_at: number }>();
      const oldVersion = (current?.state_version ?? 0) + 1;
      const newVersion = oldVersion + 1;
      const oldReportedAt = Math.max(Date.now(), (current?.reported_at ?? 0) + 1);
      const library = (label: string) => ({
        revision: `library-${label}-${newVersion}`,
        scanned_at: Math.floor(oldReportedAt / 1000),
        root_label: "Videos",
        items: [],
        categories: [],
        warnings: [],
        ready: true,
        marker: label,
        ...(paddingBytes === 0 || label === "old" ? {} : { padding: "x".repeat(paddingBytes) }),
      });
      const oldSnapshot = localSnapshot({
        state_version: oldVersion,
        reported_at: oldReportedAt,
        library: { ...library("old"), revision: `library-old-${oldVersion}` },
        operations: [{ seq: oldVersion, status: "old" }],
      }) as unknown as LocalSnapshot;
      const newSnapshot = localSnapshot({
        state_version: newVersion,
        reported_at: oldReportedAt + 1,
        library: library("new"),
        operations: [{ seq: newVersion, status: "new" }],
      }) as unknown as LocalSnapshot;

      await storeSnapshot(testEnv.DB, oldSnapshot);
      const racingDb = snapshotReadRaceDb(testEnv.DB, async () => {
        await storeSnapshot(testEnv.DB, newSnapshot);
      });
      const response = await worker.fetch(new Request(`${workerUrl}${path}`, {
        headers: { "x-telegram-init-data": owner },
      }), { ...testEnv, DB: racingDb });

      expect(response.status).toBe(200);
      if (path === "/api/state") {
        const state = await responseJson<{
          state_version: number;
          reported_at: number;
          library: { revision: string; marker: string };
          operations: Array<{ status: string }>;
        }>(response);
        expect(state.state_version).toBe(newVersion);
        expect(state.reported_at).toBe(oldReportedAt + 1);
        expect(state.library).toMatchObject({ revision: `library-new-${newVersion}`, marker: "new" });
        expect(state.operations).toEqual([{ seq: newVersion, status: "new" }]);
      } else {
        const state = await responseJson<{
          reported_at: number;
          library: { revision: string; marker: string };
        }>(response);
        expect(state.reported_at).toBe(oldReportedAt + 1);
        expect(state.library).toMatchObject({ revision: `library-new-${newVersion}`, marker: "new" });
      }
    }
  });

  it("rejects valid-JSON snapshot chunks whose stored canonical hash no longer matches", async () => {
    const current = await testEnv.DB.prepare(
      "SELECT state_version, reported_at FROM snapshot_meta WHERE id = 1",
    ).first<{ state_version: number; reported_at: number }>();
    const snapshot = localSnapshot({
      state_version: (current?.state_version ?? 0) + 1,
      reported_at: Math.max(Date.now(), (current?.reported_at ?? 0) + 1),
      library: {
        revision: "library-before-corruption",
        scanned_at: Math.floor(Date.now() / 1000),
        root_label: "Videos",
        items: [],
        categories: [],
        warnings: [],
        ready: true,
      },
    }) as unknown as LocalSnapshot;
    await storeSnapshot(testEnv.DB, snapshot);

    const chunk = await testEnv.DB.prepare(
      "SELECT data FROM snapshot_chunks WHERE kind = 'library' AND part = 0",
    ).first<{ data: string }>();
    expect(chunk).not.toBeNull();
    const library = JSON.parse(chunk?.data ?? "null") as Record<string, unknown>;
    const corruptedJson = JSON.stringify({ ...library, revision: "valid-json-with-a-different-hash" });
    await testEnv.DB.prepare(
      "UPDATE snapshot_chunks SET data = ? WHERE kind = 'library' AND part = 0",
    ).bind(corruptedJson).run();

    const response = await ownerRequest("/api/library", await signInitData());
    expect(response.status).toBe(503);
    expect(await responseJson<{ error: string }>(response)).toEqual({ error: "Service unavailable" });
  });

  it("requires the shared local authorization for every local endpoint", async () => {
    const response = await SELF.fetch(new Request(`${workerUrl}/api/local/requests`));
    const state = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(localSnapshot({ state_version: 0 })),
    }));
    expect(response.status).toBe(401);
    expect(state.status).toBe(401);
    expect(state.headers.get("X-Cloud-State-Version")).toBeNull();
  });

  it("caps the local request page at the shared 100-entry batch size", async () => {
    const defaultPage = await SELF.fetch(new Request(`${workerUrl}/api/local/requests`, { headers: localHeaders() }));
    const oversizedPage = await localRequests(101);
    expect(defaultPage.status).toBe(200);
    expect(oversizedPage.status).toBe(400);
    expect((await responseJson<{ error: string }>(oversizedPage)).error).toContain("1 to 100");
  });

  it("stores the first incomplete scan health state without reporting an empty ready library", async () => {
    const owner = await signInitData();
    const initial = await ownerRequest("/api/state", owner);
    expect((await responseJson<{ synced: boolean }>(initial)).synced).toBe(false);

    const snapshot = localSnapshot({
      library: { revision: "not-scanned", scanned_at: 0, root_label: "Videos", items: [], categories: [], warnings: [], ready: false },
    });
    const stored = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(snapshot),
    }));
    expect(stored.status).toBe(200);
    const response = await ownerRequest("/api/state", owner);
    const state = await responseJson<{ synced: boolean; library: { ready: boolean; scanned_at: number }; last_seen: number }>(response);
    expect(state.synced).toBe(true);
    expect(state.library.ready).toBe(false);
    expect(state.library.scanned_at).toBe(0);
    expect(state.last_seen).toBeGreaterThan(0);
  });

  it("stores a valid library snapshot with more than 6,700 media items", async () => {
    const base = localSnapshot();
    const library = base.library as Record<string, unknown>;
    const items = Array.from({ length: 6_701 }, (_, index) => ({
      id: `media-${index}`,
      title: `Video ${index}`,
      relative_path: `Videos/${index}.mp4`,
      bytes: 1_024,
      width: null,
      height: null,
      codec: null,
      source_ids: [],
      collection: null,
      attachments: [],
      confidence: "high",
      metadata_status: "complete",
      integrity_status: "verified",
      presence: "present",
    }));
    const body = JSON.stringify({ ...base, library: { ...library, items } });
    expect(textEncoder.encode(body).byteLength).toBeLessThan(MAX_STATE_BODY_BYTES);

    const response = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body,
    }));
    expect(response.status).toBe(200);

    const owner = await signInitData();
    const stored = await responseJson<{ synced: boolean; library: { items: unknown[] } }>(await ownerRequest("/api/library", owner));
    expect(stored.synced).toBe(true);
    expect(stored.library.items).toHaveLength(6_701);
  });

  it("returns 413 for snapshot complexity while retaining bounded default and depth behavior", async () => {
    const base = localSnapshot();
    const library = base.library as Record<string, unknown>;
    const nodeHeavyBody = JSON.stringify({
      ...base,
      library: { ...library, items: Array.from({ length: 1_000_000 }, () => null) },
    });
    expect(textEncoder.encode(nodeHeavyBody).byteLength).toBeLessThan(MAX_STATE_BODY_BYTES);

    const nodeHeavy = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: nodeHeavyBody,
    }));
    expect(nodeHeavy.status).toBe(413);
    expect(await responseJson<{ error: string }>(nodeHeavy)).toEqual({ error: "JSON structure is too complex" });

    let deepValue: unknown = null;
    for (let depth = 0; depth < 50; depth += 1) deepValue = [deepValue];
    const tooDeep = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify({ ...base, library: { ...library, items: [], unexpected: deepValue } }),
    }));
    expect(tooDeep.status).toBe(413);
    expect(await responseJson<{ error: string }>(tooDeep)).toEqual({ error: "JSON structure is too complex" });

    const oversizedBody = JSON.stringify({ ...base, padding: "x".repeat(MAX_STATE_BODY_BYTES) });
    expect(textEncoder.encode(oversizedBody).byteLength).toBeGreaterThan(MAX_STATE_BODY_BYTES);
    const oversized = await SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: oversizedBody,
    }));
    expect(oversized.status).toBe(413);
    expect(await responseJson<{ error: string }>(oversized)).toEqual({ error: "Request body is too large" });
  });

  it("keeps state_version monotonic, accepts identical retries, and rejects same-version mutations", async () => {
    const sendState = (snapshot: Record<string, unknown>) => SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(snapshot),
    }));
    const firstBody = localSnapshot();
    const first = await sendState(firstBody);
    const retry = await sendState({ ...firstBody, reported_at: Number(firstBody.reported_at) + 5_000 });
    const changedSameVersion = await sendState({ ...firstBody, operations: [{ seq: 1, status: "done" }] });
    const oldVersion = await sendState({ ...firstBody, state_version: 0 });
    const newer = await sendState({ ...firstBody, state_version: 2, operations: [{ seq: 2, status: "done" }] });
    const lateOlder = await sendState(firstBody);
    expect(first.status).toBe(200);
    expect(retry.status).toBe(200);
    expect(changedSameVersion.status).toBe(409);
    expect(changedSameVersion.headers.get("X-Cloud-State-Version")).toBe("1");
    expect(oldVersion.status).toBe(400);
    expect(oldVersion.headers.get("X-Cloud-State-Version")).toBeNull();
    expect(newer.status).toBe(200);
    expect(lateOlder.status).toBe(409);
    expect(lateOlder.headers.get("X-Cloud-State-Version")).toBe("2");
  });

  it("returns the actual newer version when a concurrent snapshot supersedes a write", async () => {
    const sendState = (snapshot: Record<string, unknown>) => SELF.fetch(new Request(`${workerUrl}/api/local/state`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(snapshot),
    }));
    const current = localSnapshot();
    expect((await sendState(current)).status).toBe(200);
    await testEnv.DB.prepare(
      "CREATE TRIGGER supersede_snapshot_during_write AFTER INSERT ON snapshot_chunks WHEN NEW.kind = 'library' BEGIN UPDATE snapshot_meta SET state_version = 51 WHERE id = 1; END",
    ).run();
    const library = current.library as Record<string, unknown>;
    const superseded = await sendState({
      ...current,
      state_version: 2,
      library: { ...library, revision: "library-hash-concurrent" },
    });
    expect(superseded.status).toBe(409);
    expect(superseded.headers.get("X-Cloud-State-Version")).toBe("51");
    expect(await responseJson<{ error: string }>(superseded)).toEqual({ error: "Snapshot was superseded by a newer local state" });
  });

  it.each([100, 500])("atomically acknowledges %i inbox sequences without exceeding D1's bind limit", async (count) => {
    for (let offset = 0; offset < count; offset += 90) {
      const rows = Array.from({ length: Math.min(90, count - offset) }, (_, index) => {
        const sequence = offset + index + 1;
        return testEnv.DB.prepare("INSERT INTO inbox (dedup_key, payload_hash, kind, payload_json, created_at) VALUES (?, ?, 'library_scan', '{}', ?)")
          .bind(`seed:${sequence}`, sequence.toString(16).padStart(64, "0"), Date.now());
      });
      await testEnv.DB.batch(rows);
    }
    const response = await SELF.fetch(new Request(`${workerUrl}/api/local/ack`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(await acknowledgmentBody(Array.from({ length: count }, (_, index) => index + 1))),
    }));
    expect(response.status).toBe(200);
    expect((await responseJson<{ acked: number }>(response)).acked).toBe(count);
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox WHERE acked_at IS NOT NULL").first<{ count: number }>())?.count).toBe(count);
  });

  it("prunes only aged acknowledged replay keys in bounded passes during authenticated polling", async () => {
    const expiredAt = Date.now() - ACKED_INBOX_REPLAY_RETENTION_MS - 60_000;
    const expiredRows = Array.from({ length: ACKED_INBOX_PRUNE_BATCH_SIZE + 3 }, (_, index) =>
      testEnv.DB.prepare("INSERT INTO inbox (dedup_key, payload_hash, kind, payload_json, created_at, acked_at) VALUES (?, ?, 'library_scan', '{}', ?, ?)")
        .bind(`expired:${index}`, `hash-expired-${index}`, expiredAt, expiredAt),
    );
    await testEnv.DB.batch(expiredRows);
    const recentAt = Date.now() - ACKED_INBOX_REPLAY_RETENTION_MS + 60_000;
    await testEnv.DB.prepare(
      "INSERT INTO inbox (dedup_key, payload_hash, kind, payload_json, created_at, acked_at) VALUES ('recent:1', 'hash-recent', 'library_scan', '{}', ?, ?)",
    ).bind(expiredAt, recentAt).run();
    await testEnv.DB.prepare(
      "INSERT INTO inbox (dedup_key, payload_hash, kind, payload_json, created_at) VALUES ('pending:1', 'hash-pending', 'library_scan', '{}', ?)",
    ).bind(expiredAt).run();

    const firstPoll = await localRequests();
    expect(firstPoll.status).toBe(200);
    expect((await responseJson<{ requests: Array<{ seq: number }> }>(firstPoll)).requests).toHaveLength(1);
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox WHERE dedup_key LIKE 'expired:%'").first<{ count: number }>())?.count)
      .toBe(3);
    expect((await testEnv.DB.prepare("SELECT acked_at FROM inbox WHERE dedup_key = 'recent:1'").first<{ acked_at: number | null }>())?.acked_at)
      .toBe(recentAt);
    expect((await testEnv.DB.prepare("SELECT acked_at FROM inbox WHERE dedup_key = 'pending:1'").first<{ acked_at: number | null }>())?.acked_at)
      .toBeNull();

    const secondPoll = await localRequests();
    expect(secondPoll.status).toBe(200);
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox WHERE dedup_key LIKE 'expired:%'").first<{ count: number }>())?.count)
      .toBe(0);
    const reused = await enqueue(testEnv.DB, "expired:0", "library_scan", {});
    expect(reused.duplicate).toBe(false);
  });

  it("keeps a durable ACK successful when best-effort retention cleanup fails", async () => {
    const expiredAt = Date.now() - ACKED_INBOX_REPLAY_RETENTION_MS - 60_000;
    await testEnv.DB.prepare(
      "INSERT INTO inbox (dedup_key, payload_hash, kind, payload_json, created_at, acked_at) VALUES ('expired:blocked', 'hash-expired', 'library_scan', '{}', ?, ?)",
    ).bind(expiredAt, expiredAt).run();
    await testEnv.DB.prepare(
      "INSERT INTO inbox (dedup_key, payload_hash, kind, payload_json, created_at) VALUES ('pending:ack', ?, 'library_scan', '{}', ?)",
    ).bind("b".repeat(64), Date.now()).run();
    await testEnv.DB.prepare(
      "CREATE TRIGGER fail_inbox_retention_delete BEFORE DELETE ON inbox WHEN OLD.dedup_key = 'expired:blocked' BEGIN SELECT RAISE(ABORT, 'retention unavailable'); END",
    ).run();
    const pending = await testEnv.DB.prepare("SELECT seq FROM inbox WHERE dedup_key = 'pending:ack'").first<{ seq: number }>();
    expect(pending).not.toBeNull();

    const response = await SELF.fetch(new Request(`${workerUrl}/api/local/ack`, {
      method: "POST",
      headers: { ...localHeaders(), "content-type": "application/json" },
      body: JSON.stringify(await acknowledgmentBody([pending!.seq])),
    }));
    expect(response.status).toBe(200);
    expect(await responseJson<{ acked: number }>(response)).toEqual({ acked: 1 });
    expect((await testEnv.DB.prepare("SELECT acked_at FROM inbox WHERE dedup_key = 'pending:ack'").first<{ acked_at: number | null }>())?.acked_at)
      .not.toBeNull();
    expect((await testEnv.DB.prepare("SELECT COUNT(*) AS count FROM inbox WHERE dedup_key = 'expired:blocked'").first<{ count: number }>())?.count)
      .toBe(1);
  });

  it("serves the static Mini App at the stable /file/ path", async () => {
    const response = await SELF.fetch(new Request(`${workerUrl}/file/`));
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("Video Library");
  });

  it("serves the independent settings Mini App at slashless and slash paths", async () => {
    const slashless = await SELF.fetch(new Request(`${workerUrl}/settings`));
    expect(slashless.status).toBe(200);
    expect(await slashless.text()).toContain("Register a media library");
    const response = await SELF.fetch(new Request(`${workerUrl}/settings/`));
    expect(response.status).toBe(200);
    const html = await response.text();
    expect(html).toContain("Register a media library");
    expect(html).toContain("/settings/app.js");
    expect(html).toContain("/file/");
  });
});

describe("offline action queue helpers", () => {
  it("refuses a new action at the bound instead of silently discarding an older action", () => {
    const initial = Array.from({ length: ACTION_QUEUE_LIMIT }, (_, index) => ({ request_id: `request-${index}`, kind: "library_scan", payload: {} }));
    const result = appendAction(initial, { request_id: "request-new", kind: "file_preview", payload: {} });
    expect(result.ok).toBe(false);
    expect(result.queue).toEqual(initial);
    expect(result.queue).toHaveLength(ACTION_QUEUE_LIMIT);
  });

  it("removes only the acknowledged ID from a freshly read queue", () => {
    const first = { request_id: "request-first", kind: "library_scan", payload: {} };
    const concurrent = { request_id: "request-concurrent", kind: "file_preview", payload: { item_ids: ["item-a"] } };
    const queue = appendAction([first], concurrent).queue;
    expect(removeQueuedAction(queue, first.request_id)).toEqual([concurrent]);
  });

  it("retries network, timeout, throttling, and server failures but not permanent 4xx responses", () => {
    expect(shouldRetryActionFailure(new TypeError("Failed to fetch"))).toBe(true);
    for (const status of [408, 429, 500, 503]) {
      const error = new HttpResponseError(status, "Temporary failure");
      expect(error.status).toBe(status);
      expect(shouldRetryActionFailure(error)).toBe(true);
    }
    for (const status of [400, 401, 403, 404, 413, 422, 600]) {
      expect(shouldRetryActionFailure(new HttpResponseError(status, "Rejected"))).toBe(false);
    }
  });

  it("keeps mapping choices across repeated legacy operation renders", () => {
    const selections = new Map();
    const initial = getOrCreateLegacySelection(selections, "legacy:4:manual:0", {
      selected: false,
      item_id: "",
      title: true,
      source_ids: true,
      collection: false,
    });
    updateLegacySelection(selections, "legacy:4:manual:0", { selected: true, item_id: "present-video" });
    updateLegacySelection(selections, "legacy:4:manual:0", { collection: true });

    const afterRender = getOrCreateLegacySelection(selections, "legacy:4:manual:0", {
      selected: false,
      item_id: "",
      title: true,
      source_ids: true,
      collection: false,
    });
    expect(afterRender).toBe(initial);
    expect(afterRender).toEqual({ selected: true, item_id: "present-video", title: true, source_ids: true, collection: true });
  });

  it("filters saved legacy candidates against current present media", () => {
    const candidates = [
      { item_id: "present-video", hint_index: 0 },
      { item_id: "missing-video", hint_index: 1 },
      { item_id: "unavailable-video", hint_index: 2 },
      { item_id: "deleted-video", hint_index: 3 },
    ];
    const currentItems = [
      { id: "present-video", presence: "present" },
      { id: "missing-video", presence: "missing" },
      { id: "unavailable-video", presence: "unavailable" },
    ];

    expect(filterPresentLegacyCandidates(candidates, currentItems)).toEqual([candidates[0]]);
    expect(filterPresentLegacyCandidates(candidates, null)).toEqual([]);
  });

  it("deduplicates waiting entries by sequence and keeps the richest local operation", () => {
    const merged = mergeRequestEntries(
      [{ seq: 1, kind: "file_confirm", status: "waiting_for_local" }, { seq: 2, kind: "legacy_import", status: "waiting_for_local" }],
      [{ seq: 1, kind: "file_confirm", status: "received" }],
      [{ seq: 1, kind: "file_confirm", status: "partial_failure", result: { items: [{ item_id: "video" }] } }],
    );
    expect(merged).toHaveLength(2);
    expect(merged[0]?.status).toBe("partial_failure");
    expect(merged[0]?.result).toEqual({ items: [{ item_id: "video" }] });
    expect(merged[1]?.seq).toBe(2);
  });

  it("renders a cloud-only request once and replaces a matching cloud summary with one local entry", () => {
    const cloudOnly = buildWaitingEntries(
      [{ seq: 7, kind: "file_preview", status: "waiting_for_local" }],
      [],
      [],
    );
    expect(cloudOnly.cloud).toHaveLength(1);
    expect(cloudOnly.local).toHaveLength(0);

    const alreadyReceived = buildWaitingEntries(
      [{ seq: 8, kind: "file_confirm", status: "waiting_for_local" }],
      [{ seq: 8, kind: "file_confirm", status: "received" }],
      [{ seq: 8, kind: "file_confirm", status: "partial_failure", result: { items: [{ item_id: "video" }] } }],
    );
    expect(alreadyReceived.cloud).toHaveLength(0);
    expect(alreadyReceived.local).toHaveLength(1);
    expect(alreadyReceived.local[0]?.status).toBe("partial_failure");
  });

  it("limits move selections to present media", () => {
    const items = [
      { id: "present", presence: "present" },
      { id: "missing", presence: "missing" },
      { id: "unavailable", presence: "unavailable" },
      { id: "unknown" },
    ];
    expect(isPresentMedia(items[0])).toBe(true);
    expect(isPresentMedia(items[1])).toBe(false);
    expect(presentMediaItems(items).map((item) => item.id)).toEqual(["present"]);
  });

  it("keeps failed file previews in history while hiding active previews already shown for confirmation", () => {
    const active = { seq: 1, kind: "file_preview", status: "awaiting_confirmation", result: { preview: { revision: 1 } } };
    const failedWithoutPreview = { seq: 2, kind: "file_preview", status: "failed", error: "Selected item is missing" };
    const failedWithPreview = { seq: 3, kind: "file_preview", status: "failed", result: { preview: { revision: 2 } }, error: "Preview became stale" };
    const other = { seq: 4, kind: "library_scan", status: "completed" };
    expect(isReviewablePreviewStatus(active.status)).toBe(true);
    expect(isReviewablePreviewStatus(failedWithPreview.status)).toBe(false);
    expect(operationHistoryEntries([active, failedWithoutPreview, failedWithPreview, other])).toEqual([
      failedWithoutPreview,
      failedWithPreview,
      other,
    ]);
  });

  it("does not count terminal file previews as waiting", () => {
    for (const status of [
      "confirmed",
      "completed",
      "cancelled",
      "expired",
      "failed",
      "uncertain",
      "partial_failure",
      "stale",
      "rejected",
    ]) {
      expect(isWaitingEntryPending({ kind: "file_preview", status }), status).toBe(false);
    }
    for (const status of ["waiting_for_local", "awaiting_confirmation", "processing", "retryable"]) {
      expect(isWaitingEntryPending({ kind: "file_preview", status }), status).toBe(true);
    }
    expect(isWaitingEntryPending({ kind: "file_confirm", status: "received" })).toBe(true);
    expect(isWaitingEntryPending({ kind: "file_confirm", status: "completed" })).toBe(false);
  });

  it("requires a fresh preview when the saved details were omitted", () => {
    expect(isIncompletePreview({
      preview_id: "preview-a",
      revision: "revision-a",
      status: "awaiting_confirmation",
      preview_incomplete: true,
      incomplete: true,
      reload_required: true,
      reason: "Preview details were omitted from the cloud snapshot.",
      preview: null,
    })).toBe(true);
    expect(isIncompletePreview({ incomplete: true, omitted_count: 3, reload_required: true })).toBe(true);
    expect(isIncompletePreview({ preview: { items: [], metadata_patches: [] } })).toBe(false);
    expect(isIncompletePreview({ preview: null })).toBe(true);
  });

  it("shows when saved operation details were omitted from history", () => {
    expect(operationDetailsWarning({
      kind: "legacy_import",
      details_incomplete: true,
      result_truncated: true,
      result: { hints_summary: "omitted" },
    })).toContain("submit the import again");
    expect(operationDetailsWarning({
      kind: "file_confirm",
      result: { operation: { details_incomplete: true } },
    })).toContain("omitted from the saved history");
    expect(operationDetailsWarning({
      kind: "legacy_import",
      result: { hints: [], candidates: [] },
    })).toBeNull();
  });
});
