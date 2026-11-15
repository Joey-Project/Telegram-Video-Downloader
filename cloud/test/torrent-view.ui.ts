import { describe, expect, it, vi } from "vitest";
import { createTorrentView, TORRENT_DRAFT_STORAGE_KEY } from "../public/file/torrent-view.js";
import type { TorrentOperation } from "../public/file/torrent-view.js";
import { validateTorrentActionPayload } from "../src/torrent_actions";

const HASH = "a".repeat(40);
const TASK_ID = "task-owner-1";

function memoryStorage(): Storage {
  const values = new Map<string, string>();
  return {
    get length() { return values.size; },
    clear: () => values.clear(),
    getItem: (key) => values.get(key) ?? null,
    key: (index) => [...values.keys()][index] ?? null,
    removeItem: (key) => values.delete(key),
    setItem: (key, value) => values.set(key, String(value)),
  };
}

interface SentAction {
  payload: Record<string, unknown>;
  requestId: string;
}

function sentAt(actions: SentAction[], index = 0): SentAction {
  const action = actions.at(index);
  if (!action) throw new Error(`Expected action at index ${index}.`);
  return action;
}

function actionOperation(requestId: string, seq: number, result: Record<string, unknown>, status = "completed"): TorrentOperation {
  return { seq, kind: "torrent_action", request_id: requestId, status, result };
}

function detailResult({
  page = 0,
  folderId = 0,
  inventoryRevision = 1,
  selectionRevision = 1,
  selectedIndexes = [],
  entries,
  scopeIndexes = [0, 1, 2],
  hasMore = false,
}: {
  page?: number;
  folderId?: number;
  inventoryRevision?: number;
  selectionRevision?: number;
  selectedIndexes?: number[];
  entries?: Array<Record<string, unknown>>;
  scopeIndexes?: number[];
  hasMore?: boolean;
} = {}) {
  return {
    operation: "detail",
    task: {
      task_id: TASK_ID,
      name: "Example torrent",
      phase: "downloading",
      state: "downloading",
      progress: 0.42,
      inventory_revision: inventoryRevision,
      selection_revision: selectionRevision,
      selected_indexes: selectedIndexes,
      selected_count: selectedIndexes.length,
      import_count: 0,
      error: "",
    },
    page,
    page_size: 50,
    total: 51,
    has_more: hasMore,
    folder_id: folderId,
    query: "",
    filter: "music",
    scope_indexes: scopeIndexes,
    entries: entries ?? [{
      kind: "folder", id: 1, path: "Album", indexes: [0, 1],
    }, {
      kind: "file", index: 2, path: "Album/Disc 1/01.flac", size: 4096,
      progress: 0.25, complete: false, selected: false, supported_music: true,
      needs_reconfirm: false, music_stage: "waiting_for_download",
    }],
  };
}

function reviewResult(indexes: number[]) {
  return {
    operation: "review",
    review_id: "review-owner-1",
    task_id: TASK_ID,
    inventory_revision: 1,
    selection_revision: 1,
    selected_indexes: indexes,
    selected_count: indexes.length,
    total_bytes: indexes.length * 4096,
    ready_count: 0,
    waiting_count: indexes.length,
    existing_import_count: 0,
    removed_pending_count: 0,
    entries: indexes.slice(0, 50).map((index) => ({
      kind: "file", index, path: `Album/${index}.flac`, size: 4096, progress: 0.25,
      complete: false, selected: true, supported_music: true, needs_reconfirm: false,
      music_stage: "waiting_for_download",
    })),
    more_count: Math.max(0, indexes.length - 50),
  };
}

function mount() {
  document.body.innerHTML = "<main><div id='torrent-root'></div></main>";
  const root = document.querySelector<HTMLElement>("#torrent-root")!;
  const submit = vi.fn(async (_payload: Record<string, unknown>, _requestId: string) => true);
  const storage = memoryStorage();
  const taskRoutes: Array<[string, string]> = [];
  let operations: TorrentOperation[] = [];
  const view = createTorrentView({
    root, submit, storage,
    onTaskOpen: (taskRef, type) => taskRoutes.push([type, taskRef]),
  });
  const sent = (): SentAction[] => submit.mock.calls.map(([payload, requestId]) => ({ payload, requestId }));
  const publish = (requestId: string, seq: number, result: Record<string, unknown>, extra: Pick<TorrentOperation, "details_incomplete" | "result_truncated"> = {}) => {
    operations = [...operations, { ...actionOperation(requestId, seq, result), ...extra }];
    view.setSnapshot({
      operations,
      pending_requests: [],
    });
  };
  const republish = () => view.setSnapshot({ operations, pending_requests: [] });
  const settle = async () => { await Promise.resolve(); await Promise.resolve(); };
  return { root, submit, storage, taskRoutes, view, sent, publish, republish, settle };
}

describe("torrent selection Mini App", () => {
  it("opens catalog after the owner gate, navigates by hash, and keeps drafts across pages and search", async () => {
    const ui = mount();
    expect(ui.submit).not.toHaveBeenCalled();
    ui.view.open();
    await ui.settle();
    const catalogRequest = sentAt(ui.sent());
    expect(catalogRequest.payload).toEqual({ operation: "catalog", page: 0, query: "" });
    expect(() => validateTorrentActionPayload(catalogRequest.payload)).not.toThrow();
    ui.publish(catalogRequest.requestId, 10, {
      operation: "catalog", page: 0, page_size: 50, total: 1, has_more: false,
      query: "", rows: [{ hash: HASH, name: "Example torrent", state: "downloading", progress: 0.42, size: 8192 }],
    });
    expect(ui.root.textContent).toContain("Example torrent");
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-open]")!.click();
    await ui.settle();
    expect(ui.taskRoutes).toEqual([["hash", HASH]]);
    const detailRequest = ui.sent().at(-1)!;
    expect(detailRequest.payload).toEqual({
      operation: "detail", hash: HASH, page: 0, folder_id: 0, query: "", filter: "music",
    });
    expect(() => validateTorrentActionPayload(detailRequest.payload)).not.toThrow();
    ui.publish(detailRequest.requestId, 11, detailResult({ hasMore: true }));
    expect(ui.root.textContent).toContain("Downloading · 42.0%");
    expect(ui.root.textContent).not.toContain("Inventory r1");
    expect(ui.root.textContent).not.toMatch(/Folder \d+/u);
    expect(ui.root.textContent).toContain("Task root");
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-select-scope]")!.click();
    expect(ui.root.textContent).toContain("3 selected");
    const stored = JSON.parse(ui.storage.getItem(TORRENT_DRAFT_STORAGE_KEY) ?? "{}");
    expect(stored[TASK_ID].indexes).toEqual([0, 1, 2]);

    ui.root.querySelector<HTMLButtonElement>('[data-torrent-detail-page="1"]')!.click();
    await ui.settle();
    const secondPageRequest = ui.sent().at(-1)!;
    expect(secondPageRequest.payload).toMatchObject({ operation: "detail", hash: HASH, page: 1 });
    ui.publish(secondPageRequest.requestId, 12, detailResult({
      page: 1, entries: [{ kind: "file", index: 50, path: "Bonus/hidden.flac", size: 8192,
        progress: 1, complete: true, selected: false, supported_music: true, needs_reconfirm: false, music_stage: "ready" }],
      scopeIndexes: [50],
    }));
    expect(ui.root.textContent).toContain("3 selected");
    expect(ui.root.querySelector<HTMLInputElement>('[data-torrent-file="50"]')).not.toBeNull();

    const search = ui.root.querySelector<HTMLInputElement>('[data-torrent-detail-search] input[name="query"]')!;
    search.value = "bonus";
    ui.root.querySelector<HTMLFormElement>("[data-torrent-detail-search]")!.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    await ui.settle();
    const searchRequest = ui.sent().at(-1)!;
    expect(searchRequest.payload).toMatchObject({ operation: "detail", query: "bonus", page: 0, hash: HASH });
    expect(() => validateTorrentActionPayload(searchRequest.payload)).not.toThrow();
    ui.publish(searchRequest.requestId, 13, detailResult({ page: 0, entries: [] }));
    expect(ui.root.textContent).toContain("3 selected");
    expect(ui.submit).toHaveBeenCalledTimes(4);
    ui.view.dispose();
  });

  it.each(["while the POST is pending", "after the POST is accepted"])(
    "keeps late detail results scoped to the task selected after navigation %s",
    async (submitTiming) => {
    const ui = mount();
    const hashA = "a".repeat(40);
    const hashB = "b".repeat(40);
    ui.view.open();
    await ui.settle();
    const catalog = sentAt(ui.sent());
    ui.publish(catalog.requestId, 10, {
      operation: "catalog", page: 0, page_size: 50, total: 2, has_more: false, query: "",
      rows: [
        { hash: hashA, name: "Task A", state: "downloading", progress: 0.25, size: 4096 },
        { hash: hashB, name: "Task B", state: "downloading", progress: 0.75, size: 8192 },
      ],
    });

    let finishTaskASubmit!: (queued: boolean) => void;
    if (submitTiming === "while the POST is pending") {
      ui.submit.mockImplementationOnce(() => new Promise<boolean>((resolve) => { finishTaskASubmit = resolve; }));
    }
    ui.root.querySelector<HTMLButtonElement>(`[data-torrent-open="${hashA}"]`)!.click();
    const detailA = sentAt(ui.sent(), 1);
    expect(detailA.payload).toMatchObject({ operation: "detail", hash: hashA });
    if (submitTiming === "after the POST is accepted") await ui.settle();
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-back]")!.click();
    expect(ui.root.querySelector<HTMLButtonElement>(`[data-torrent-open="${hashB}"]`)?.disabled).toBe(false);
    expect(ui.root.querySelector<HTMLButtonElement>(`[data-torrent-control][data-torrent-hash="${hashB}"]`)?.disabled)
      .toBe(submitTiming === "while the POST is pending");
    ui.root.querySelector<HTMLButtonElement>(`[data-torrent-open="${hashB}"]`)!.click();
    await ui.settle();
    expect(ui.view.currentTaskId()).toBe(hashB);
    if (submitTiming === "while the POST is pending") {
      expect(ui.sent()).toHaveLength(2);
      expect(ui.root.textContent).toContain("Waiting for the previous request to finish");
      finishTaskASubmit(true);
      await ui.settle();
    }
    expect(ui.sent()).toHaveLength(3);
    const detailB = sentAt(ui.sent(), 2);
    expect(detailB.payload).toMatchObject({ operation: "detail", hash: hashB });

    ui.publish(detailA.requestId, 11, {
      ...detailResult({ entries: [{ kind: "file", index: 3, path: "Task A/old-track.flac", size: 4096,
        progress: 1, complete: true, selected: true, supported_music: true,
        needs_reconfirm: false, music_stage: "ready" }], scopeIndexes: [3] }),
      task: { ...detailResult().task, task_id: "task-A", name: "Task A", selected_indexes: [3], selected_count: 1 },
    });
    expect(ui.root.textContent).not.toContain("Task A/old-track.flac");
    expect(ui.root.querySelector('[data-torrent-file="3"]')).toBeNull();
    expect(ui.root.textContent).toContain("Waiting for task files");

    ui.publish(detailB.requestId, 12, {
      ...detailResult({ entries: [{ kind: "file", index: 4, path: "Task B/current-track.flac", size: 8192,
        progress: 1, complete: true, selected: true, supported_music: true,
        needs_reconfirm: false, music_stage: "ready" }], scopeIndexes: [4] }),
      task: { ...detailResult().task, task_id: "task-B", name: "Task B", selected_indexes: [4], selected_count: 1 },
    });
    expect(ui.root.textContent).toContain("Task B");
    expect(ui.root.textContent).toContain("Task B/current-track.flac");
    expect(ui.root.textContent).not.toContain("Task A/old-track.flac");
    expect(ui.root.querySelector<HTMLInputElement>('[data-torrent-file="4"]')?.checked).toBe(true);
    const savedDrafts = JSON.parse(ui.storage.getItem(TORRENT_DRAFT_STORAGE_KEY) ?? "{}");
    expect(savedDrafts["task-B"].indexes).toEqual([4]);
    expect(savedDrafts["task-A"]).toBeUndefined();

    ui.publish(detailA.requestId, 11, {
      ...detailResult({ entries: [{ kind: "file", index: 3, path: "Task A/old-track.flac", size: 4096,
        progress: 1, complete: true, selected: true, supported_music: true,
        needs_reconfirm: false, music_stage: "ready" }], scopeIndexes: [3] }),
      task: { ...detailResult().task, task_id: "task-A", name: "Task A", selected_indexes: [3], selected_count: 1 },
    });
    expect(ui.root.textContent).toContain("Task B/current-track.flac");
    expect(ui.root.textContent).not.toContain("Task A/old-track.flac");
    expect(ui.root.querySelector<HTMLInputElement>('[data-torrent-file="4"]')?.checked).toBe(true);
    expect(JSON.parse(ui.storage.getItem(TORRENT_DRAFT_STORAGE_KEY) ?? "{}")["task-B"].indexes).toEqual([4]);
    ui.view.dispose();
  });

  it("cancels read-only review without confirming, then confirms only the exact saved review", async () => {
    const ui = mount();
    ui.view.open(TASK_ID);
    await ui.settle();
    const detailRequest = sentAt(ui.sent());
    expect(detailRequest.payload).toMatchObject({ operation: "detail", task_id: TASK_ID });
    expect(() => validateTorrentActionPayload(detailRequest.payload)).not.toThrow();
    ui.publish(detailRequest.requestId, 20, detailResult());
    ui.root.querySelector<HTMLButtonElement>('[data-torrent-file="2"]')!.click();
    await ui.settle();
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-review]")!.click();
    await ui.settle();
    const reviewRequest = ui.sent().at(-1)!;
    expect(reviewRequest.payload).toEqual({
      operation: "review", task_id: TASK_ID, inventory_revision: 1, selection_revision: 1,
      selected_indexes: [2],
    });
    expect(() => validateTorrentActionPayload(reviewRequest.payload)).not.toThrow();
    ui.publish(reviewRequest.requestId, 21, reviewResult([2]));
    expect(ui.root.textContent).toContain("READ-ONLY REVIEW");
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-review-cancel]")!.click();
    expect(ui.root.textContent).not.toContain("READ-ONLY REVIEW");
    expect(ui.root.textContent).toContain("1 selected");
    expect(ui.sent().some(({ payload }) => payload.operation === "confirm")).toBe(false);

    ui.root.querySelector<HTMLButtonElement>("[data-torrent-review]")!.click();
    await ui.settle();
    const secondReview = ui.sent().at(-1)!;
    ui.publish(secondReview.requestId, 22, reviewResult([2]));
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-confirm]")!.click();
    await ui.settle();
    const confirm = ui.sent().at(-1)!;
    expect(confirm.payload).toEqual({ operation: "confirm", review_seq: 22, review_id: "review-owner-1" });
    expect(() => validateTorrentActionPayload(confirm.payload)).not.toThrow();
    expect(ui.sent().some(({ payload }) => payload.operation === "review" && "selected_indexes" in payload)).toBe(true);
    ui.view.dispose();
  });

  it("rejects same-sequence results carrying another request ID", async () => {
    const detailUi = mount();
    detailUi.view.open(TASK_ID);
    await detailUi.settle();
    const detail = sentAt(detailUi.sent());
    detailUi.view.setSnapshot({
      operations: [actionOperation("previous-epoch-detail", 24, detailResult({
        entries: [{ kind: "file", index: 98, path: "FORGED DETAIL RESULT.flac", size: 4096,
          progress: 1, complete: true, selected: false, supported_music: true,
          needs_reconfirm: false, music_stage: "ready" }],
        scopeIndexes: [98],
      }))],
      pending_requests: [],
    });
    detailUi.view.recordReceipt(detail.requestId, { seq: 24 });
    expect(detailUi.root.textContent).toContain("belongs to a different request");
    expect(detailUi.root.textContent).not.toContain("FORGED DETAIL RESULT.flac");
    expect(detailUi.root.querySelector('[data-torrent-file="98"]')).toBeNull();
    detailUi.view.dispose();

    const reviewUi = mount();
    reviewUi.view.open(TASK_ID);
    await reviewUi.settle();
    const validDetail = sentAt(reviewUi.sent());
    reviewUi.publish(validDetail.requestId, 30, detailResult());
    reviewUi.root.querySelector<HTMLButtonElement>('[data-torrent-file="2"]')!.click();
    reviewUi.root.querySelector<HTMLButtonElement>("[data-torrent-review]")!.click();
    await reviewUi.settle();
    const review = sentAt(reviewUi.sent(), 1);
    const forgedReview = {
      ...reviewResult([2]),
      entries: [{ kind: "file", index: 2, path: "FORGED REVIEW RESULT.flac", size: 4096,
        progress: 1, complete: true, selected: true, supported_music: true,
        needs_reconfirm: false, music_stage: "ready" }],
    };
    reviewUi.view.setSnapshot({
      operations: [
        actionOperation(validDetail.requestId, 30, detailResult()),
        actionOperation("previous-epoch-review", 31, forgedReview),
      ],
      pending_requests: [],
    });
    reviewUi.view.recordReceipt(review.requestId, { seq: 31 });
    expect(reviewUi.root.textContent).toContain("belongs to a different request");
    expect(reviewUi.root.textContent).not.toContain("FORGED REVIEW RESULT.flac");
    expect(reviewUi.root.querySelector("[data-torrent-confirm]")).toBeNull();
    expect(reviewUi.sent().some(({ payload }) => payload.operation === "confirm")).toBe(false);
    reviewUi.view.dispose();

    const legacyUi = mount();
    legacyUi.view.open(TASK_ID);
    await legacyUi.settle();
    const legacyDetail = sentAt(legacyUi.sent());
    legacyUi.view.recordReceipt(legacyDetail.requestId, { seq: 40 });
    const legacyOperation: TorrentOperation = {
      seq: 40, kind: "torrent_action", status: "completed", result: detailResult(),
    };
    legacyUi.view.setSnapshot({ operations: [legacyOperation], pending_requests: [] });
    expect(legacyUi.root.textContent).toContain("Album/Disc 1/01.flac");
    expect(legacyUi.root.textContent).not.toContain("belongs to a different request");
    legacyUi.view.dispose();
  });

  it("allows an explicit empty review to clear saved selections and rejects stale drafts visibly", async () => {
    const ui = mount();
    ui.view.open(TASK_ID);
    await ui.settle();
    const detail = sentAt(ui.sent());
    ui.publish(detail.requestId, 30, detailResult({ selectedIndexes: [0, 1] }));
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-clear-scope]")!.click();
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-review]")!.click();
    await ui.settle();
    const review = ui.sent().at(-1)!;
    expect(review.payload).toMatchObject({ operation: "review", selected_indexes: [] });
    expect(() => validateTorrentActionPayload(review.payload)).not.toThrow();

    ui.publish(review.requestId, 31, reviewResult([]));
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-review-cancel]")!.click();
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-detail-refresh]")!.click();
    await ui.settle();
    const refreshed = ui.sent().at(-1)!;
    ui.publish(refreshed.requestId, 32, detailResult({ inventoryRevision: 2, selectionRevision: 2, selectedIndexes: [0] }));
    expect(ui.root.textContent).toContain("Task files or saved selection changed");
    expect(ui.root.querySelector<HTMLButtonElement>("[data-torrent-review]")!.disabled).toBe(true);
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-rebase]")!.click();
    expect(ui.root.textContent).not.toContain("Task files or saved selection changed");
    expect(ui.root.textContent).toContain("1 selected");
    ui.view.dispose();
  });

  it("shows incomplete and failed results, validates pause/resume payloads, and starts no idle actions", async () => {
    const ui = mount();
    ui.view.open();
    await ui.settle();
    const first = sentAt(ui.sent());
    ui.publish(first.requestId, 40, {
      operation: "catalog", page: 0, page_size: 50, total: 1, has_more: false, query: "",
      rows: [{ hash: HASH, name: "Paused torrent", state: "pausedDL", progress: 0.8, size: 10 }],
    });
    expect(ui.root.textContent).toContain("Resume");
    const before = ui.submit.mock.calls.length;
    ui.republish();
    expect(ui.submit).toHaveBeenCalledTimes(before);
    ui.root.querySelector<HTMLButtonElement>("[data-torrent-control]")!.click();
    await ui.settle();
    const control = ui.sent().at(-1)!;
    expect(control.payload).toEqual({ operation: "control", hash: HASH, control: "resume" });
    expect(() => validateTorrentActionPayload(control.payload)).not.toThrow();

    ui.publish(control.requestId, 41, { operation: "control", hash: HASH, control: "resume", applied: true });
    expect(ui.sent().at(-1)?.payload).toMatchObject({ operation: "catalog", page: 0, query: "" });

    const refresh = ui.sent().at(-1)!;
    ui.publish(refresh.requestId, 42, { operation: "catalog", page: 0, page_size: 50, total: 1, has_more: false, query: "", rows: [] }, { details_incomplete: true });
    expect(ui.root.textContent).toContain("incomplete or expired");
    expect(() => validateTorrentActionPayload({ operation: "control", hash: HASH, control: "pause", actor_user_id: "1" })).toThrow();
    expect(() => validateTorrentActionPayload({ operation: "catalog", page: 0, query: "\u0000" })).toThrow();
    ui.view.dispose();
  });
});
