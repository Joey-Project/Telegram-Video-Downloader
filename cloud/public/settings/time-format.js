function relativeTimeAgo(value, unit, now) {
  const raw = Number(value);
  if (!Number.isFinite(raw) || raw <= 0) return "Not yet";

  const timestamp = unit === "seconds" ? raw * 1_000 : raw;
  if (!Number.isFinite(timestamp) || timestamp <= 0) return "Not yet";

  const seconds = Math.max(0, Math.floor((now - timestamp) / 1_000));
  if (seconds < 60) return "Just now";
  if (seconds < 3_600) return `${Math.floor(seconds / 60)} min ago`;
  if (seconds < 86_400) return `${Math.floor(seconds / 3_600)} hr ago`;
  return `${Math.floor(seconds / 86_400)} days ago`;
}

export function timeAgoMilliseconds(value, now = Date.now()) {
  return relativeTimeAgo(value, "milliseconds", now);
}

export function timeAgoSeconds(value, now = Date.now()) {
  return relativeTimeAgo(value, "seconds", now);
}
