import { escapeHtml } from "./preview-rendering.js";

const ROOT_BINDING_PATTERN = /^v1:[0-9a-f]{64}$/u;
const MAX_SUMMARY_ITEMS = 4;
const MAX_SAMPLE_FILES_PER_ITEM = 2;
const MAX_SAMPLE_METADATA_PATCHES = 4;
const DECIMAL_BYTE_COUNT_PATTERN = /^(?:0|[1-9][0-9]{0,19})$/u;

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

function validDirectory(value) {
  return typeof value === "string" && !/[\u0000-\u001f\u007f]/u.test(value);
}

function validCount(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function identityField(values, field, validate) {
  const present = values.filter((value) => Object.hasOwn(value, field)).map((value) => value[field]);
  if (present.length === 0 || present.some((value) => !validate(value))) return null;
  return present.every((value) => value === present[0]) ? present[0] : null;
}

function validLibraryId(value) {
  return validIdentifier(value) && new TextEncoder().encode(value).byteLength <= 128;
}

function validRootBinding(value) {
  return typeof value === "string" && ROOT_BINDING_PATTERN.test(value);
}

function summaryPreview(entry) {
  const result = objectValue(entry?.result);
  const resultPreview = objectValue(result?.preview);
  const directPreview = objectValue(entry?.preview);
  for (const value of [resultPreview, directPreview, objectValue(entry)]) {
    if (value?.file_preview_summary === true) return value;
  }
  return null;
}

function sampleFile(value) {
  const file = objectValue(value);
  if (!file || !validText(file.source_path) || !validText(file.target_path)) return null;
  const validBytes = validCount(file.bytes)
    || typeof file.bytes === "string" && DECIMAL_BYTE_COUNT_PATTERN.test(file.bytes);
  return validBytes ? { sourcePath: file.source_path, targetPath: file.target_path, bytes: file.bytes } : null;
}

function sampleItem(value) {
  const item = objectValue(value);
  if (!item || !validText(item.item_id)
      || !validText(item.source_path) || !validText(item.target_path)
      || !["move", "skip", "no_op"].includes(item.outcome)
      || !validCount(item.file_count) || !validCount(item.omitted_file_count)
      || !Array.isArray(item.sample_files) || item.sample_files.length > MAX_SAMPLE_FILES_PER_ITEM
      || item.omitted_file_count !== item.file_count - item.sample_files.length) return null;
  const files = item.sample_files.map(sampleFile);
  if (files.some((file) => file === null)) return null;
  return {
    itemId: item.item_id,
    sourcePath: item.source_path,
    targetPath: item.target_path,
    outcome: item.outcome,
    fileCount: item.file_count,
    omittedFileCount: item.omitted_file_count,
    files,
  };
}

function sampleMetadataPatch(value) {
  const patch = objectValue(value);
  if (!patch || !validText(patch.item_id) || !validText(patch.nfo_path) || typeof patch.changed !== "boolean") return null;
  return { itemId: patch.item_id, nfoPath: patch.nfo_path, changed: patch.changed };
}

/**
 * Validates a versioned compact file preview without treating its samples as
 * the execution plan. Legacy incomplete/truncated flags are deliberately not
 * rejected here; callers may only use this result after current library
 * identity and availability checks.
 */
export function normalizeFilePreviewSummary(entry) {
  const operation = objectValue(entry);
  const result = objectValue(operation?.result);
  const summary = summaryPreview(operation);
  if (!operation || !summary || operation.kind !== "file_preview"
      || String(operation.status ?? "").toLowerCase() !== "awaiting_confirmation"
      || summary.summary_version !== 1) return null;
  const compactReceiptFlags = result
    && operation.details_incomplete === true && operation.result_truncated === true
    && result.details_incomplete === true && result.result_truncated === true;
  const compactPreviewFlags = operation.incomplete === true
    && operation.preview_incomplete === true && operation.reload_required === true;
  if (!compactReceiptFlags && !compactPreviewFlags) return null;

  const identitySources = [operation, objectValue(operation.recovery_identity), result]
    .filter(Boolean);
  const previewId = identityField(identitySources, "preview_id", validIdentifier);
  const revision = identityField(identitySources, "revision", validIdentifier);
  const libraryId = identityField(identitySources, "library_id", validLibraryId);
  const rootBinding = identityField(identitySources, "root_binding", validRootBinding);
  if (!previewId || !revision || !libraryId || !rootBinding
      || summary.id !== previewId || summary.revision !== revision
      || summary.library_id !== libraryId || summary.root_binding !== rootBinding) return null;

  const selectedCount = summary.selected_count;
  const moveItemCount = summary.move_item_count;
  const fileCount = summary.file_count;
  const metadataPatchCount = summary.metadata_patch_count;
  const changedMetadataPatchCount = summary.changed_metadata_patch_count;
  const conflictCount = summary.conflict_count;
  const omittedItemCount = summary.omitted_item_count;
  const omittedFileCount = summary.omitted_file_count;
  const omittedMetadataPatchCount = summary.omitted_metadata_patch_count;
  const totalBytes = summary.total_bytes;
  const outcomeCounts = objectValue(summary.outcome_counts);
  const sampleItems = summary.sample_items;
  const sampleMetadataPatches = summary.sample_metadata_patches;
  if (summary.file_preview_summary !== true
      || !validCount(selectedCount) || !validCount(moveItemCount) || !validCount(fileCount)
      || !validCount(metadataPatchCount) || !validCount(changedMetadataPatchCount)
      || changedMetadataPatchCount > metadataPatchCount || !validCount(conflictCount)
      || !validCount(omittedItemCount) || !validCount(omittedFileCount)
      || !validCount(omittedMetadataPatchCount)
      || typeof totalBytes !== "string" || !DECIMAL_BYTE_COUNT_PATTERN.test(totalBytes)
      || !outcomeCounts || !["move", "skip", "no_op"].every((key) => validCount(outcomeCounts[key]))
      || Object.keys(outcomeCounts).some((key) => !["move", "skip", "no_op"].includes(key))
      || outcomeCounts.move + outcomeCounts.skip + outcomeCounts.no_op !== moveItemCount
      || conflictCount !== outcomeCounts.skip
      || !Array.isArray(sampleItems) || sampleItems.length > MAX_SUMMARY_ITEMS
      || omittedItemCount !== moveItemCount - sampleItems.length
      || !Array.isArray(sampleMetadataPatches) || sampleMetadataPatches.length > MAX_SAMPLE_METADATA_PATCHES
      || omittedMetadataPatchCount !== metadataPatchCount - sampleMetadataPatches.length
      || typeof summary.metadata_only !== "boolean"
      || summary.metadata_only && summary.target_relative_dir !== null
      || !summary.metadata_only && !validDirectory(summary.target_relative_dir)
      || typeof summary.rename !== "boolean"
      || !["skip", "keep_both"].includes(summary.conflict)) return null;

  const items = sampleItems.map(sampleItem);
  const patches = sampleMetadataPatches.map(sampleMetadataPatch);
  if (items.some((item) => item === null) || patches.some((patch) => patch === null)) return null;
  const sampleFileCount = items.reduce((total, item) => total + item.files.length, 0);
  if (sampleFileCount > fileCount || omittedFileCount !== fileCount - sampleFileCount) return null;

  return {
    id: previewId,
    revision,
    libraryId,
    rootBinding,
    selectedCount,
    metadataOnly: summary.metadata_only,
    targetRelativeDir: summary.target_relative_dir,
    rename: summary.rename,
    conflict: summary.conflict,
    moveItemCount,
    fileCount,
    totalBytes,
    outcomeCounts: { move: outcomeCounts.move, skip: outcomeCounts.skip, noOp: outcomeCounts.no_op },
    conflictCount,
    metadataPatchCount,
    changedMetadataPatchCount,
    omittedItemCount,
    omittedFileCount,
    omittedMetadataPatchCount,
    sampleItems: items,
    sampleMetadataPatches: patches,
  };
}

function count(value) {
  return value.toLocaleString();
}

function plural(value, noun) {
  return `${count(value)} ${noun}${value === 1 ? "" : "s"}`;
}

function byteTotal(value) {
  return `${BigInt(value).toLocaleString()} bytes`;
}

function outcomeLabel(outcome) {
  return outcome === "skip" ? "Will skip" : outcome === "no_op" ? "No change" : "Will move";
}

/** Renders only bounded sample rows; it never creates controls from their paths. */
export function renderFilePreviewSummary(summary) {
  if (!summary || !Array.isArray(summary.sampleItems) || !Array.isArray(summary.sampleMetadataPatches)) return "";
  const sampleFiles = summary.sampleItems.reduce((total, item) => total + item.files.length, 0);
  const destination = summary.metadataOnly
    ? "Metadata only · file locations stay the same"
    : summary.targetRelativeDir || "Library root";
  const metadataSummary = summary.metadataPatchCount > 0
    ? `<p>${count(summary.changedMetadataPatchCount)} changed metadata patches · ${count(summary.metadataPatchCount)} total</p>`
    : "";
  const itemRows = summary.sampleItems.map((item) => {
    const files = item.files.length
      ? `<ul>${item.files.map((file) => `<li><code>${escapeHtml(file.sourcePath)} → ${escapeHtml(file.targetPath)}</code> · ${escapeHtml(file.bytes)} bytes</li>`).join("")}</ul>`
      : "<p class=\"field-hint\">No attached-file paths are shown for this sample item.</p>";
    const omitted = item.omittedFileCount > 0
      ? `<p class="field-hint">${count(item.omittedFileCount)} attached-file paths omitted for this sample item.</p>`
      : "";
    return `<div class="preview-line"><span><strong>Sample item · ${escapeHtml(item.itemId)}</strong><br><span class="field-hint">${escapeHtml(item.sourcePath)} → ${escapeHtml(item.targetPath)}</span>${files}${omitted}</span><strong class="${item.outcome === "skip" ? "skip" : "move"}">${outcomeLabel(item.outcome)}</strong></div>`;
  }).join("");
  const patchRows = summary.sampleMetadataPatches.map((patch) => (
    `<div class="preview-line"><span><strong>Sample metadata · ${escapeHtml(patch.itemId)}</strong><br><span class="field-hint">${escapeHtml(patch.nfoPath)}</span></span><strong class="${patch.changed ? "move" : "skip"}">${patch.changed ? "Will update" : "No change"}</strong></div>`
  )).join("");
  const omittedItems = summary.omittedItemCount > 0
    ? `${count(summary.omittedItemCount)} item details omitted`
    : "No item details omitted";
  const omittedFiles = summary.omittedFileCount > 0
    ? `${count(summary.omittedFileCount)} attached-file paths omitted`
    : "No attached-file paths omitted";
  const omittedPatches = summary.omittedMetadataPatchCount > 0
    ? `${count(summary.omittedMetadataPatchCount)} metadata patch details omitted`
    : "No metadata patch details omitted";
  const sampleItemsLabel = plural(summary.sampleItems.length, "sample media item");
  const sampleFilesLabel = plural(sampleFiles, "sample attached-file path");
  const fullBatch = summary.metadataOnly
    ? `Confirm will apply the metadata update to all ${plural(summary.selectedCount, "selected item")}.`
    : `Confirm will apply the saved plan to all ${plural(summary.selectedCount, "selected item")}.`;

  return `<div class="file-preview-summary"><p><strong>Complete saved batch</strong> · ${escapeHtml(fullBatch)}</p><p>${plural(summary.selectedCount, "selected item")} · ${plural(summary.moveItemCount, "move item")} · ${count(summary.outcomeCounts.move)} move · ${count(summary.outcomeCounts.skip)} skip · ${count(summary.outcomeCounts.noOp)} no change</p><p>${plural(summary.fileCount, "file")} · ${byteTotal(summary.totalBytes)} · ${plural(summary.conflictCount, "conflict")}</p>${metadataSummary}<p>Destination: ${escapeHtml(destination)} · Rename: ${summary.rename ? "Yes" : "No"} · If a file already exists: ${summary.conflict === "keep_both" ? "Keep both" : "Skip it"}</p><p class="field-hint">Showing ${sampleItemsLabel} and ${sampleFilesLabel}. ${omittedItems}; ${omittedFiles}; ${omittedPatches}. Sample paths may be abbreviated. These rows are examples only; they are not the full plan.</p><div class="preview-table">${itemRows || "<div class=\"preview-line\"><span>No sample media items are available.</span></div>"}${patchRows}</div></div>`;
}

export function filePreviewSummaryConfirmLabel(summary) {
  if (!summary) return "Confirm changes";
  const action = summary.metadataOnly ? "Confirm metadata update" : "Confirm changes";
  return `${action} for all ${plural(summary.selectedCount, "selected item")}`;
}
