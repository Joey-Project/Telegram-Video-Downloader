export type TorrentFilter = "music" | "all" | "ready" | "waiting" | "imported";

export interface TorrentCatalogRow {
  hash: string;
  name: string;
  state: string;
  progress: number;
  size: number;
}

export interface TorrentCatalogResult {
  operation: "catalog";
  page: number;
  page_size: number;
  total: number;
  has_more: boolean;
  query: string;
  rows: TorrentCatalogRow[];
}

export interface TorrentTaskSummary {
  task_id: string;
  name: string;
  phase: string;
  state: string | null;
  progress: number;
  inventory_revision: number;
  selection_revision: number;
  selected_indexes: number[];
  selected_count: number;
  import_count: number;
  error: string | null;
}

export interface TorrentFileEntry {
  kind: "file";
  index: number;
  path: string;
  size: number;
  progress: number;
  complete: boolean;
  selected: boolean;
  supported_music: boolean;
  needs_reconfirm: boolean;
  music_stage: string | null;
}

export interface TorrentFolderEntry {
  kind: "folder";
  id: number;
  path: string;
  indexes: number[];
}

export type TorrentEntry = TorrentFileEntry | TorrentFolderEntry;

export interface TorrentDetailResult {
  operation: "detail";
  task: TorrentTaskSummary;
  page: number;
  page_size: number;
  total: number;
  has_more: boolean;
  folder_id: number;
  query: string;
  filter: TorrentFilter;
  scope_indexes: number[];
  entries: TorrentEntry[];
}

export interface TorrentReviewResult {
  operation: "review";
  review_id: string;
  task_id: string;
  inventory_revision: number;
  selection_revision: number;
  selected_indexes: number[];
  selected_count: number;
  total_bytes: number;
  ready_count: number;
  waiting_count: number;
  existing_import_count: number;
  removed_pending_count: number;
  entries: TorrentFileEntry[];
  more_count: number;
}

export interface TorrentOperation {
  seq: number;
  kind: string;
  request_id?: string;
  status: string;
  error?: string | null;
  details_incomplete?: boolean;
  result_truncated?: boolean;
  result?: Record<string, unknown>;
}

export interface TorrentSnapshot {
  operations?: TorrentOperation[];
  pending_requests?: Array<{ seq: number; kind: string; request_id?: string; status?: string }>;
}

export interface TorrentViewOptions {
  root: HTMLElement;
  submit(payload: Record<string, unknown>, requestId: string): Promise<boolean>;
  onMessage?(message: string, level?: "info" | "error"): void;
  onTaskBack?(): void;
  onTaskOpen?(taskRef: string, refType: "task_id" | "hash"): void;
  storage?: Storage | null;
}

export interface TorrentView {
  open(taskId?: string, hash?: string): void;
  setSnapshot(snapshot: TorrentSnapshot | null): void;
  render(): void;
  hasPendingResult(): boolean;
  currentTaskId(): string;
  currentTaskRefType(): "task_id" | "hash";
  recordReceipt(requestId: string, receipt: { seq?: number }): void;
  dispose(): void;
}

export const TORRENT_PAGE_SIZE: number;
export const TORRENT_DRAFT_STORAGE_KEY: string;
export function createTorrentView(options: TorrentViewOptions): TorrentView;
