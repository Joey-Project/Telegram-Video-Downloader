import { webcrypto } from "node:crypto";

export const TEST_DATABASE_EPOCH = "b".repeat(32);
const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

function splitUtf8(text: string, chunkBytes: number): string[] {
  const bytes = encoder.encode(text);
  const chunks: string[] = [];
  let start = 0;
  while (start < bytes.byteLength) {
    let end = Math.min(start + chunkBytes, bytes.byteLength);
    if (end < bytes.byteLength) {
      while (end > start && (bytes[end]! & 0xc0) === 0x80) end -= 1;
    }
    if (end === start) throw new Error("Test chunk size is too small for a UTF-8 code point");
    chunks.push(decoder.decode(bytes.subarray(start, end)));
    start = end;
  }
  return chunks;
}

async function sha256(value: string): Promise<string> {
  const bytes = encoder.encode(value);
  const digest = await webcrypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function makeChunkedSnapshot(
  snapshot: Record<string, any>,
  { chunkBytes = 32 * 1024, databaseEpoch = TEST_DATABASE_EPOCH }: { chunkBytes?: number; databaseEpoch?: string } = {},
): Promise<{ manifest: Record<string, any>; chunks: Map<string, string> }> {
  if (chunkBytes < 4 || chunkBytes > 32 * 1024) throw new Error("Test chunk size is outside the allowed range");
  const {
    library,
    state_version: stateVersion = 1,
    reported_at: reportedAt = 1_800_000_001,
    synced = true,
    last_seen: lastSeen = 1_800_000_000,
    stale = false,
    pending_requests: pendingRequests = [],
    ...state
  } = snapshot;
  const parts = new Map<string, string>();
  const describe = async (value: unknown) => {
    const json = JSON.stringify(value);
    const textChunks = splitUtf8(json, chunkBytes);
    const descriptors = [];
    for (const data of textChunks) {
      const sha256Hex = await sha256(data);
      descriptors.push({ sha256: sha256Hex, bytes: encoder.encode(data).byteLength });
      parts.set(sha256Hex, data);
    }
    return { bytes: encoder.encode(json).byteLength, chunks: descriptors };
  };
  const [libraryDescriptor, stateDescriptor] = await Promise.all([describe(library), describe(state)]);
  return {
    manifest: {
      format: "chunked-v1",
      database_epoch: databaseEpoch,
      state_version: stateVersion,
      reported_at: reportedAt,
      library_revision: library.revision,
      library: libraryDescriptor,
      state: stateDescriptor,
      synced,
      last_seen: lastSeen,
      stale,
      pending_requests: pendingRequests,
    },
    chunks: parts,
  };
}
