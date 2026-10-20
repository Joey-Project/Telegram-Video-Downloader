export async function submitLegacyFileImport(file, submit, reject, maxBytes) {
  if (file.size > maxBytes) {
    reject("Legacy text exceeds the 256 KiB limit.");
    return false;
  }

  const text = await file.text();
  const bytes = new TextEncoder().encode(text).byteLength;
  if (bytes === 0) {
    reject("Paste legacy text or choose a text file first.");
    return false;
  }
  if (bytes > maxBytes) {
    reject("Legacy text exceeds the 256 KiB limit.");
    return false;
  }

  submit(text);
  return true;
}
