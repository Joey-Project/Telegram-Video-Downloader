export function takeRejectedSettingsActions(localActions, requestIds) {
  if (!(localActions instanceof Map) || !Array.isArray(requestIds)) return [];
  const settled = [];
  for (const requestId of requestIds) {
    if (typeof requestId !== "string") continue;
    const local = localActions.get(requestId);
    if (!local || local.rejected !== true) continue;
    localActions.delete(requestId);
    settled.push({
      requestId,
      kind: typeof local.kind === "string" ? local.kind : "request",
      label: typeof local.label === "string" && local.label.trim() ? local.label : "Settings request",
      error: typeof local.error === "string" && local.error.trim() ? local.error : "Request rejected",
    });
  }
  return settled;
}
