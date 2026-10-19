export function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/gu, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[character]);
}

export function renderMovePreviewItems(items) {
  if (!Array.isArray(items)) return "";
  return items.map((item) => {
    const outcome = String(item?.outcome ?? "move");
    const label = outcome === "skip" ? "Skipped" : outcome === "no_op" ? "No change" : "Move";
    const files = Array.isArray(item?.files) ? item.files : [];
    const fileDetails = files.length
      ? `<details class="preview-file-details" open><summary>Review ${files.length} exact source → destination path${files.length === 1 ? "" : "s"}</summary><ul>${files.map((file) => `<li><code>${escapeHtml(file?.source_path ?? "Source path unavailable")} → ${escapeHtml(file?.target_path ?? "Destination path unavailable")}</code></li>`).join("")}</ul></details>`
      : "";
    return `<div class="preview-line"><div><span>${escapeHtml(item?.source_path ?? item?.item_id ?? "Selected file")} → ${escapeHtml(item?.target_path ?? "Library root")}</span>${fileDetails}${item?.reason ? `<br><span class="preview-reason">${escapeHtml(item.reason)}</span>` : ""}</div><strong class="${outcome === "skip" ? "skip" : "move"}">${label}</strong></div>`;
  }).join("");
}
