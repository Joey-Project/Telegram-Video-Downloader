export const MAX_TARGET_RELATIVE_DIR_BYTES = 1024;

function hasUnpairedSurrogate(value) {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const nextCodeUnit = value.charCodeAt(index + 1);
      if (!(nextCodeUnit >= 0xdc00 && nextCodeUnit <= 0xdfff)) return true;
      index += 1;
    } else if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      return true;
    }
  }
  return false;
}

export function validateTargetRelativeDirectory(value) {
  const directory = String(value ?? "").trim().replaceAll("\\", "/");
  if (hasUnpairedSurrogate(directory)) {
    return { directory, error: "Destination folder must contain valid Unicode text." };
  }
  if (new TextEncoder().encode(directory).byteLength > MAX_TARGET_RELATIVE_DIR_BYTES) {
    return { directory, error: "Destination folder must not exceed 1,024 UTF-8 bytes." };
  }
  if (/[\u0000-\u001f\u007f]/u.test(directory)) {
    return { directory, error: "Use a safe relative folder path without control characters." };
  }
  if (directory === "") return { directory, error: null };

  const segments = directory.split("/");
  if (directory.startsWith("/") || /^[A-Za-z]:/u.test(directory) || segments.some((segment) => {
    const lowercaseSegment = segment.toLowerCase();
    return segment.length === 0 || segment === "." || segment === ".." ||
      lowercaseSegment.startsWith(".telegram-video-downloader-library") ||
      lowercaseSegment.startsWith(".telegram-video-downloader-");
  })) {
    return { directory, error: "Use a safe relative folder path without empty, private, . or .. segments." };
  }
  return { directory, error: null };
}
