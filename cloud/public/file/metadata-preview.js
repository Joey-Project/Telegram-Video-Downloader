const FIELD_LABELS = {
  title: "Title",
  collection: "Collection",
  order: "Order",
  part_id: "Part ID",
};

export function metadataPreviewDetails(patch) {
  const details = [{ kind: "status", text: patch?.changed ? "NFO will be updated" : "No metadata change" }];
  const changes = Array.isArray(patch?.changes) ? patch.changes : [];
  for (const change of changes) {
    if (!change || typeof change !== "object" || typeof change.after !== "string") continue;
    details.push({
      kind: "change",
      label: FIELD_LABELS[change.field] ?? String(change.field ?? "Metadata"),
      before: typeof change.before === "string" ? change.before : null,
      after: change.after,
    });
  }
  const sourceIds = Array.isArray(patch?.added_source_ids)
    ? patch.added_source_ids.filter((value) => typeof value === "string")
    : [];
  if (sourceIds.length) details.push({ kind: "change", label: "Source IDs to add", before: null, after: sourceIds.join(", ") });
  return details;
}
