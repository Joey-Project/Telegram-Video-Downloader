export const RECENT_OPERATION_SNAPSHOT_LIMIT = 100;
export const RECENT_SNAPSHOT_MAX_AGE_MS = 10 * 60 * 1000;

export function inspectDownloadSubmissionHistory(submission, snapshot) {
  if (!Number.isSafeInteger(submission?.seq) || submission.seq <= 0) return { kind: "unaccepted" };

  const operations = Array.isArray(snapshot?.operations) ? snapshot.operations : null;
  if (!operations) return { kind: "unknown" };

  const operation = operations.find((entry) => entry?.kind === "download_submit" && entry.seq === submission.seq);
  if (operation) return { kind: "recorded", operation };

  const pendingRequests = Array.isArray(snapshot?.pending_requests) ? snapshot.pending_requests : null;
  if (!pendingRequests) return { kind: "unknown" };
  const pending = pendingRequests.find((entry) => entry?.seq === submission.seq);
  if (pending) return { kind: "pending", pending };

  const lastSeen = snapshot.last_seen;
  const now = Date.now();
  if (snapshot.synced !== true || snapshot.stale !== false
      || !Number.isSafeInteger(lastSeen) || lastSeen > now
      || now - lastSeen > RECENT_SNAPSHOT_MAX_AGE_MS
      || operations.length !== RECENT_OPERATION_SNAPSHOT_LIMIT) {
    return { kind: "unknown" };
  }

  let previousSequence = submission.seq;
  for (const entry of operations) {
    if (typeof entry?.kind !== "string" || typeof entry.status !== "string"
        || !Number.isSafeInteger(entry.seq) || entry.seq <= previousSequence) {
      return { kind: "unknown" };
    }
    previousSequence = entry.seq;
  }

  return { kind: "outside_recent_history" };
}

export function releaseDownloadSubmissionTracking(state, snapshot) {
  const submission = state?.downloadSubmission;
  if (inspectDownloadSubmissionHistory(submission, snapshot).kind !== "outside_recent_history") {
    return false;
  }
  state.downloadSubmission = null;
  const requestIdentity = typeof submission.request_id === "string" && submission.request_id.length > 0
    ? `Request ${submission.request_id} (sequence ${submission.seq})`
    : `Request sequence ${submission.seq}`;
  state.downloadMessage = `${requestIdentity} is outside recent history. Its result is unknown. This only clears this browser's waiting status; it does not cancel or resend the task. Check your computer before adding it again.`;
  return true;
}
