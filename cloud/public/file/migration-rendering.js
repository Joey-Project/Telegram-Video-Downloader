import { migrationControlActions, migrationHistoryNotice } from "./migration-conflicts.js";
import { escapeHtml } from "./preview-rendering.js";

export const MAX_VISIBLE_MIGRATIONS = 8;

function formatBytes(value) {
  const bytes = Number(value);
  if (!Number.isFinite(bytes) || bytes < 0) return "Size unavailable";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let size = bytes;
  let unit = -1;
  do { size /= 1024; unit += 1; } while (size >= 1024 && unit < units.length - 1);
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unit]}`;
}

function statusText(value) {
  const text = String(value ?? "pending").replaceAll("_", " ");
  return text.replace(/^\w/u, (letter) => letter.toLocaleUpperCase());
}

function countValue(value, fallback = 0) {
  return Number.isSafeInteger(value) && value >= 0 ? value : fallback;
}

function renderMigrationProgress(migration, libraries) {
  const status = String(migration.status ?? "unknown");
  const active = status === "queued" || status === "running";
  const paused = status === "paused";
  const items = Array.isArray(migration.items) ? migration.items : [];
  const resultRows = items.slice(0, 40).map((item) => {
    const itemStatus = String(item?.status ?? "unknown");
    const outcome = itemStatus === "complete" ? "Moved · now at target" : itemStatus === "skipped" ? "Skipped · source retained" : itemStatus === "failed" ? "Failed · source retained" : statusText(itemStatus);
    const sourcePath = item?.source_path ?? "Source path unavailable";
    const targetPath = item?.target_path ?? "Target path unavailable";
    const detail = `${sourcePath} → ${targetPath}${item?.error ? ` · ${item.error}` : ""}`;
    return `<div class="preview-line"><span>${escapeHtml(item?.item_id ?? "Selected item")}<br><span class="field-hint">${escapeHtml(detail)}</span></span><strong class="${itemStatus === "failed" ? "skip" : itemStatus === "complete" ? "move" : ""}">${escapeHtml(outcome)}</strong></div>`;
  }).join("");
  const progress = `${Number(migration.completed_items ?? 0).toLocaleString()}/${Number(migration.total_items ?? 0).toLocaleString()} items · ${Number(migration.completed_files ?? 0).toLocaleString()}/${Number(migration.total_files ?? 0).toLocaleString()} files · ${formatBytes(migration.copied_bytes)} / ${formatBytes(migration.total_bytes)}`;
  let counts = `${Number(migration.success_count ?? 0)} moved · ${Number(migration.skipped_count ?? 0)} skipped · ${Number(migration.failed_count ?? 0)} failed`;
  const omittedConflictOverrideCount = countValue(migration.omitted_conflict_override_count);
  if (omittedConflictOverrideCount > 0) {
    counts += " · " + omittedConflictOverrideCount.toLocaleString() + " per-item conflict choices summarized";
  }
  const transfers = Array.isArray(migration.task_transfers) ? migration.task_transfers : [];
  const transferRows = transfers.slice(0, 20).map((transfer, index) => {
    const files = `${Number(transfer.completed_files ?? 0).toLocaleString()}/${Number(transfer.total_files ?? 0).toLocaleString()} staged files copied`;
    const transferError = typeof transfer.error === "string"
      ? transfer.error.slice(0, 240) + (transfer.error_detail_truncated === true ? " · error detail shortened" : "")
      : "";
    const stateText = transferError
      ? `Needs attention · ${transferError}`
      : transfer.commit_acked === true
        ? transfer.source_cleanup_complete === true
          ? "Task handoff complete"
          : "Finishing task handoff and source cleanup"
        : transfer.copy_complete === true
          ? "Files copied · finishing task handoff"
          : "Copying staged task data";
    return `<div class="preview-line"><span>Unfinished task ${index + 1}<br><span class="field-hint">${escapeHtml(files)}</span></span><strong class="${transferError ? "skip" : transfer.commit_acked === true && transfer.source_cleanup_complete === true ? "move" : ""}">${escapeHtml(stateText)}</strong></div>`;
  }).join("");
  const transferMore = transfers.length > 20 ? `<p class="field-hint">Showing 20 of ${transfers.length} unfinished task handoffs.</p>` : "";
  const incompleteTaskCount = countValue(migration.incomplete_task_count);
  const omittedTransferCount = countValue(migration.omitted_task_transfer_count);
  const omittedTransferErrorCount = countValue(migration.omitted_task_transfer_error_count);
  const reportedTransferErrorCount = countValue(migration.task_transfer_error_count);
  const taskTransferCount = countValue(migration.task_transfer_count, transfers.length + omittedTransferCount);
  const transferOmission = omittedTransferCount > 0
    ? `<div class="preview-line"><span>Task handoff details summarized<br><span class="field-hint">${omittedTransferCount.toLocaleString()} detail${omittedTransferCount === 1 ? "" : "s"} omitted from this cloud view${omittedTransferErrorCount > 0 ? ` · ${omittedTransferErrorCount.toLocaleString()} error${omittedTransferErrorCount === 1 ? "" : "s"} summarized` : ""}</span></span><strong>${reportedTransferErrorCount > 0 ? `${reportedTransferErrorCount.toLocaleString()} error${reportedTransferErrorCount === 1 ? "" : "s"}` : `${taskTransferCount.toLocaleString()} handoff${taskTransferCount === 1 ? "" : "s"}`}</strong></div>`
    : "";
  const taskHandoffStatus = incompleteTaskCount > 0 && transfers.length === 0 && omittedTransferCount === 0
    ? `<div class="preview-line"><span>Unfinished task handoffs<br><span class="field-hint">${escapeHtml(migration.awaiting_task_plans === true ? "Preparing the task transfer plans." : "Waiting for task transfer details.")}</span></span><strong>${incompleteTaskCount.toLocaleString()} task${incompleteTaskCount === 1 ? "" : "s"}</strong></div>`
    : "";
  const retryLabel = Number(migration.failed_count ?? 0) > 0 ? "Retry failed items" : "Retry task handoffs";
  const controlLabels = {
    pause: "Pause",
    resume: "Resume",
    stop_remaining: "Stop remaining",
    retry_failed: retryLabel,
  };
  const controls = migrationControlActions(migration).map((control) =>
    `<button class="small-button" type="button" data-migration-control="${control}" data-migration-id="${escapeHtml(migration.id)}">${controlLabels[control]}</button>`
  ).join("");
  const source = libraries.find((entry) => entry.id === migration.source_library_id);
  const target = libraries.find((entry) => entry.id === migration.target_library_id);
  const visibleItems = items.length > 40 ? `<p class="field-hint">Showing the first 40 of ${items.length} item results.</p>` : "";
  const showTaskTransfers = incompleteTaskCount > 0 || taskTransferCount > 0 || omittedTransferCount > 0;
  return `<article class="stack-card"><div class="stack-card-head"><div><h3>Library migration</h3><p>${escapeHtml(source?.name ?? migration.source_library_id)} → ${escapeHtml(target?.name ?? migration.target_library_id)}</p><p>${escapeHtml(progress)}</p><p>${escapeHtml(counts)}</p>${incompleteTaskCount ? `<p>${incompleteTaskCount.toLocaleString()} unfinished task handoff${incompleteTaskCount === 1 ? "" : "s"}</p>` : ""}</div><span class="state-pill${active || paused ? " pending" : ""}">${escapeHtml(statusText(status))}</span></div>${showTaskTransfers ? `<div class="preview-table task-transfer-list">${taskHandoffStatus}${transferRows}${transferOmission}${transferMore}</div>` : ""}<div class="preview-table">${resultRows}${visibleItems}</div>${controls ? `<div class="stack-actions">${controls}</div>` : ""}</article>`;
}

export function renderMigrationHistory({ migrations, libraries = [], libraryId, omittedCount }) {
  const rows = Array.isArray(migrations) ? migrations : [];
  const relevant = rows.filter((migration) => migration && typeof migration === "object"
    && (migration.source_library_id === libraryId || migration.target_library_id === libraryId));
  const notice = migrationHistoryNotice(omittedCount);
  const noticeMarkup = notice ? `<p class="field-hint">${escapeHtml(notice)}</p>` : "";
  // The backend has already ordered the bounded snapshot by recovery priority.
  return `${noticeMarkup}${relevant.slice(0, MAX_VISIBLE_MIGRATIONS).map((migration) => renderMigrationProgress(migration, libraries)).join("")}`;
}
