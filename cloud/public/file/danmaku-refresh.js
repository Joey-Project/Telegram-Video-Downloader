export const DANMAKU_REFRESH_SAMPLE_LIMIT = 24;

export function isIncompleteBoundPreview(entry) {
  return ["preview_incomplete", "incomplete", "reload_required", "details_incomplete", "result_truncated"]
    .some((field) => entry?.[field] === true);
}

export function danmakuRefreshOperationName(entry) {
  return entry?.operation ?? entry?.result?.operation ?? entry?.payload?.operation ?? null;
}

export function danmakuRefreshPreviewEntries(snapshot) {
  const operations = Array.isArray(snapshot?.operations) ? snapshot.operations : [];
  const previews = Array.isArray(snapshot?.previews) ? snapshot.previews : [];
  const entries = new Map();
  for (const entry of [...operations, ...previews]) {
    if (entry?.kind !== "danmaku_refresh" || danmakuRefreshOperationName(entry) !== "preview") continue;
    const result = entry.result && typeof entry.result === "object" ? entry.result : {};
    const preview = result.preview && typeof result.preview === "object"
      ? result.preview
      : entry.preview && typeof entry.preview === "object" ? entry.preview : null;
    const id = entry.preview_id ?? result.preview_id ?? preview?.id;
    const revision = entry.revision ?? result.revision ?? preview?.revision;
    if (typeof id !== "string" || !id || typeof revision !== "string" || !revision || !preview) continue;
    const normalized = {
      ...entry,
      kind: "danmaku_refresh",
      id,
      preview_id: id,
      revision,
      library_id: entry.library_id ?? result.library_id ?? preview.library_id ?? "",
      root_binding: entry.root_binding ?? result.root_binding ?? preview.root_binding ?? "",
      preview,
    };
    const key = Number.isSafeInteger(entry.seq) && entry.seq > 0 ? `seq:${entry.seq}` : `preview:${id}:${revision}`;
    const previous = entries.get(key);
    const isReviewable = (value) => ["awaiting_confirmation", "awaiting_review"].includes(String(value ?? "").toLowerCase());
    const incomingIsTerminal = !isReviewable(normalized.status);
    const previousIsTerminal = previous && !isReviewable(previous.status);
    if (!previous || incomingIsTerminal && !previousIsTerminal
        || !incomingIsTerminal && !previousIsTerminal
          && (!isIncompleteBoundPreview(normalized) && isIncompleteBoundPreview(previous)
            || isIncompleteBoundPreview(normalized) === isIncompleteBoundPreview(previous)
              && Array.isArray(normalized.preview?.items) && !Array.isArray(previous.preview?.items))) {
      entries.set(key, normalized);
    }
  }
  return [...entries.values()];
}

export function danmakuRefreshItemSample(preview, escapeHtml) {
  const items = Array.isArray(preview?.items) ? preview.items : [];
  const sample = items.slice(0, DANMAKU_REFRESH_SAMPLE_LIMIT);
  const rows = sample.map((item) => {
    const title = String(item?.title ?? item?.item_id ?? "Selected video");
    const path = String(item?.relative_path ?? "Path unavailable");
    const status = item?.status === "ready" ? "Ready to append" : "Skipped";
    const reason = typeof item?.reason === "string" ? `<br><span class="preview-reason">${escapeHtml(item.reason)}</span>` : "";
    return `<div class="preview-line"><span>${escapeHtml(title)}<br><span class="field-hint">${escapeHtml(path)}</span>${reason}</span><strong class="${item?.status === "ready" ? "move" : "skip"}">${status}</strong></div>`;
  }).join("");
  const total = Number.isSafeInteger(preview?.total) ? preview.total : items.length;
  const omitted = Math.max(0, total - sample.length);
  return `${rows}${omitted ? `<p class="field-hint">${omitted.toLocaleString()} additional videos are included in this preview.</p>` : ""}`;
}
