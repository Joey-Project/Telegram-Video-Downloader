import { escapeHtml } from "./preview-rendering.js";

const MAX_SAMPLE_ITEMS = 4;
const MAX_SAMPLE_WARNINGS = 2;
const FALLBACK_OPERATION_STATUSES = new Set([
  "pending", "processing", "awaiting_confirmation", "completed", "retryable", "failed", "uncertain", "stale", "cancelled",
]);

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

function validText(value) {
  return typeof value === "string" && value.trim().length > 0
    && !/[\u0000-\u001f\u007f]/u.test(value);
}

function validIdentifier(value) {
  return validText(value) && value.trim() === value;
}

function validCount(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function count(value) {
  return value.toLocaleString();
}

function operationStatusLabel(status) {
  return status === "complete" ? "Complete" : status === "partial" ? "Partial" : "Failed";
}

function fallbackStatusLabel(status) {
  return ({
    pending: "Pending",
    processing: "Processing",
    awaiting_confirmation: "Awaiting confirmation",
    completed: "Completed",
    retryable: "Retryable",
    failed: "Failed",
    uncertain: "Outcome unknown",
    stale: "Stale",
    cancelled: "Cancelled",
  })[status] ?? "Recorded";
}

function sampleItem(value) {
  const item = objectValue(value);
  if (!item || !validIdentifier(item.item_id)
      || !["move", "skip", "no_op"].includes(item.outcome)
      || !validCount(item.moved_files) || !validCount(item.total_files)
      || item.moved_files > item.total_files || typeof item.metadata_patched !== "boolean"
      || item.error !== null && !validText(item.error)) return null;
  return {
    itemId: item.item_id,
    outcome: item.outcome,
    movedFiles: item.moved_files,
    totalFiles: item.total_files,
    metadataPatched: item.metadata_patched,
    error: item.error,
  };
}

/** Validates bounded file-confirm history summaries without deriving totals from samples. */
export function normalizeFileConfirmSummary(entry) {
  const operationEntry = objectValue(entry);
  const result = objectValue(operationEntry?.result);
  const operation = objectValue(result?.operation);
  if (!operationEntry || operationEntry.kind !== "file_confirm" || !result
      || operation?.file_confirm_summary !== true || operation.summary_version !== 1) return null;

  const previewId = result.preview_id;
  const revision = result.revision;
  if (!validIdentifier(previewId) || !validIdentifier(revision)
      || operationEntry.preview_id !== undefined && operationEntry.preview_id !== previewId
      || operationEntry.revision !== undefined && operationEntry.revision !== revision) return null;

  if (operation.details_available === false) {
    if (!FALLBACK_OPERATION_STATUSES.has(operation.status) || !validText(operation.summary_message)) return null;
    return {
      id: previewId,
      revision,
      detailsAvailable: false,
      status: operation.status,
      retryableAccessError: null,
      summaryMessage: operation.summary_message,
    };
  }

  if (operationEntry.details_incomplete !== true || operationEntry.result_truncated !== true
      || result.details_incomplete !== true || result.result_truncated !== true
      || operation.id !== previewId || operation.revision !== revision
      || !["complete", "partial", "failed"].includes(operation.status)
      || typeof operation.retryable_access_error !== "boolean"
      || !validText(operation.summary_message)) return null;

  const selectedCount = operation.selected_count;
  const outcomeCounts = objectValue(operation.outcome_counts);
  const fileCount = operation.file_count;
  const movedFileCount = operation.moved_file_count;
  const metadataPatchedItemCount = operation.metadata_patched_item_count;
  const errorCount = operation.error_count;
  const warningCount = operation.warning_count;
  const omittedItemCount = operation.omitted_item_count;
  const omittedWarningCount = operation.omitted_warning_count;
  const sampleItems = operation.sample_items;
  const sampleWarnings = operation.sample_warnings;
  if (!validCount(selectedCount) || !outcomeCounts
      || !["move", "skip", "no_op"].every((key) => validCount(outcomeCounts[key]))
      || Object.keys(outcomeCounts).some((key) => !["move", "skip", "no_op"].includes(key))
      || outcomeCounts.move + outcomeCounts.skip + outcomeCounts.no_op !== selectedCount
      || !validCount(fileCount) || !validCount(movedFileCount) || movedFileCount > fileCount
      || !validCount(metadataPatchedItemCount) || metadataPatchedItemCount > selectedCount
      || !validCount(errorCount) || !validCount(warningCount)
      || !Array.isArray(sampleItems) || sampleItems.length > MAX_SAMPLE_ITEMS
      || !validCount(omittedItemCount) || omittedItemCount !== selectedCount - sampleItems.length
      || !Array.isArray(sampleWarnings) || sampleWarnings.length > MAX_SAMPLE_WARNINGS
      || !validCount(omittedWarningCount) || omittedWarningCount !== warningCount - sampleWarnings.length) return null;

  const samples = sampleItems.map(sampleItem);
  const warnings = sampleWarnings.map((warning) => validText(warning) ? warning : null);
  if (samples.some((item) => item === null) || warnings.some((warning) => warning === null)) return null;
  const sampledOutcomes = { move: 0, skip: 0, no_op: 0 };
  let sampledFiles = 0;
  let sampledMovedFiles = 0;
  let sampledPatchedItems = 0;
  for (const item of samples) {
    sampledOutcomes[item.outcome] += 1;
    sampledFiles += item.totalFiles;
    sampledMovedFiles += item.movedFiles;
    if (item.metadataPatched) sampledPatchedItems += 1;
  }
  if (Object.keys(sampledOutcomes).some((key) => sampledOutcomes[key] > outcomeCounts[key])
      || sampledFiles > fileCount || sampledMovedFiles > movedFileCount
      || sampledPatchedItems > metadataPatchedItemCount || warnings.length > warningCount) return null;

  return {
    id: previewId,
    revision,
    detailsAvailable: true,
    status: operation.status,
    retryableAccessError: operation.retryable_access_error,
    selectedCount,
    outcomeCounts: { move: outcomeCounts.move, skip: outcomeCounts.skip, noOp: outcomeCounts.no_op },
    fileCount,
    movedFileCount,
    metadataPatchedItemCount,
    errorCount,
    warningCount,
    omittedItemCount,
    omittedWarningCount,
    sampleItems: samples,
    sampleWarnings: warnings,
    summaryMessage: operation.summary_message,
  };
}

/** Renders full saved totals and clearly separated sample result rows. */
export function renderFileConfirmSummary(summary) {
  if (!summary) return "";
  if (summary.detailsAvailable === false) {
    return `<div class="operation-warnings"><p><strong>${escapeHtml(fallbackStatusLabel(summary.status))} batch record</strong> · Detailed batch results are unavailable in this snapshot. ${escapeHtml(summary.summaryMessage)}</p></div>`;
  }
  if (!Array.isArray(summary.sampleItems) || !Array.isArray(summary.sampleWarnings)) return "";

  const statusLabel = operationStatusLabel(summary.status);
  const retryable = summary.retryableAccessError
    ? `<p class="field-hint">A retryable access error occurred. The preview remains available for a retry after the library is accessible.</p>`
    : "";
  const samples = summary.sampleItems.map((item) => {
    const outcome = item.outcome === "skip" ? "Skipped" : item.outcome === "no_op" ? "No change" : "Moved";
    const error = item.error ? `<br><span class="preview-reason">${escapeHtml(item.error)}</span>` : "";
    const metadata = item.metadataPatched ? " · metadata updated" : "";
    return `<div class="preview-line"><span><strong>Sample result · ${escapeHtml(item.itemId)}</strong><br><span class="field-hint">${count(item.movedFiles)}/${count(item.totalFiles)} related files moved${metadata}</span>${error}</span><strong class="${item.error ? "skip" : item.outcome === "skip" ? "skip" : "move"}">${outcome}</strong></div>`;
  }).join("");
  const warnings = summary.sampleWarnings.length
    ? `<div class="operation-warnings">${summary.sampleWarnings.map((warning) => `<p>Sample warning: ${escapeHtml(warning)}</p>`).join("")}</div>`
    : "";
  const omissions = [
    summary.omittedItemCount > 0 ? `${count(summary.omittedItemCount)} item results omitted` : "No item results omitted",
    summary.omittedWarningCount > 0 ? `${count(summary.omittedWarningCount)} warnings omitted` : "No warnings omitted",
  ].join(" · ");
  return `<div class="file-confirm-summary"><p><strong>${statusLabel} batch result</strong> · ${count(summary.selectedCount)} item results · ${count(summary.outcomeCounts.move)} move · ${count(summary.outcomeCounts.skip)} skip · ${count(summary.outcomeCounts.noOp)} no change</p><p>${count(summary.movedFileCount)} of ${count(summary.fileCount)} related files moved · ${count(summary.metadataPatchedItemCount)} metadata updates · ${count(summary.errorCount)} item errors · ${count(summary.warningCount)} warnings</p>${retryable}<p class="field-hint">Showing ${count(summary.sampleItems.length)} sample item results. ${omissions}. Samples do not represent the full result. ${escapeHtml(summary.summaryMessage)}</p><div class="preview-table">${samples || "<div class=\"preview-line\"><span>No sample item results are available.</span></div>"}</div>${warnings}</div>`;
}
