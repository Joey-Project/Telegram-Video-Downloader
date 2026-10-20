export function sourceIdUrl(source) {
  if (typeof source !== "string") return null;
  const match = /^youtube:([A-Za-z0-9_-]{11})$/u.exec(source.trim());
  return match ? `https://www.youtube.com/watch?v=${match[1]}` : null;
}
