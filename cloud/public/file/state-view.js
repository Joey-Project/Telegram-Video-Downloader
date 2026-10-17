export function mergeRequestEntries(pendingRequests, tasks, operations) {
  const entries = new Map();
  for (const [group, values] of [["cloud", pendingRequests], ["task", tasks], ["operation", operations]]) {
    if (!Array.isArray(values)) continue;
    values.forEach((entry, index) => {
      if (!entry || typeof entry !== "object") return;
      const seq = Number(entry.seq);
      const key = Number.isSafeInteger(seq) && seq > 0 ? `seq:${seq}` : `${group}:${index}`;
      entries.set(key, entry);
    });
  }
  return [...entries.values()];
}

export function buildWaitingEntries(pendingRequests, tasks, operations) {
  const cloud = mergeRequestEntries(pendingRequests, [], []);
  const local = mergeRequestEntries([], tasks, operations);
  const localSequences = new Set(local.map((entry) => Number(entry.seq)).filter(Number.isSafeInteger));
  return {
    cloud: cloud.filter((entry) => !localSequences.has(Number(entry.seq))),
    local,
  };
}
