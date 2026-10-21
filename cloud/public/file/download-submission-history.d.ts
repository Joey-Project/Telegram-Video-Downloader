export const RECENT_OPERATION_SNAPSHOT_LIMIT: number;
export const RECENT_SNAPSHOT_MAX_AGE_MS: number;

export type DownloadSubmissionHistory =
  | { kind: "unaccepted" }
  | { kind: "unknown" }
  | { kind: "pending"; pending: Record<string, unknown> }
  | { kind: "recorded"; operation: Record<string, unknown> }
  | { kind: "outside_recent_history" };

export function inspectDownloadSubmissionHistory(
  submission: { seq?: unknown } | null | undefined,
  snapshot: {
    synced?: unknown;
    stale?: unknown;
    last_seen?: unknown;
    operations?: unknown;
    pending_requests?: unknown;
  } | null | undefined,
): DownloadSubmissionHistory;

export function releaseDownloadSubmissionTracking(
  state: {
    downloadSubmission?: { request_id?: unknown; seq?: unknown } | null;
    downloadMessage?: string | null;
  },
  snapshot: {
    synced?: unknown;
    stale?: unknown;
    last_seen?: unknown;
    operations?: unknown;
    pending_requests?: unknown;
  } | null | undefined,
): boolean;
