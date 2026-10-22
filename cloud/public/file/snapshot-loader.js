const MAX_CHUNK_BYTES = 32 * 1024;
const MAX_TOTAL_BYTES = 12 * 1024 * 1024;
const MAX_CHUNKS = 386;
const MAX_JSON_DEPTH = 48;
const MAX_JSON_NODES = 1_000_000;
const CHUNK_CONCURRENCY = 4;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const DATABASE_EPOCH_PATTERN = /^[0-9a-f]{32}$/u;

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function invalidSnapshot(message) {
  throw new Error(`Cloud snapshot is invalid: ${message}`);
}

function positiveSafeInteger(value) {
  return Number.isSafeInteger(value) && value > 0;
}

function validatePartitionDescriptor(value, name) {
  if (!isRecord(value) || !Number.isSafeInteger(value.bytes) || value.bytes < 1 || value.bytes > MAX_TOTAL_BYTES
      || !Array.isArray(value.chunks) || value.chunks.length === 0 || value.chunks.length > MAX_CHUNKS) {
    invalidSnapshot(`${name} chunk metadata is invalid`);
  }
  let bytes = 0;
  for (const chunk of value.chunks) {
    if (!isRecord(chunk) || typeof chunk.sha256 !== "string" || !SHA256_PATTERN.test(chunk.sha256)
        || !Number.isSafeInteger(chunk.bytes) || chunk.bytes < 1 || chunk.bytes > MAX_CHUNK_BYTES) {
      invalidSnapshot(`${name} chunk metadata is invalid`);
    }
    bytes += chunk.bytes;
    if (bytes > MAX_TOTAL_BYTES) invalidSnapshot("combined snapshot exceeds the byte limit");
  }
  if (bytes !== value.bytes) invalidSnapshot(`${name} byte count does not match its chunks`);
  return { bytes: value.bytes, chunks: value.chunks };
}

function validateManifest(value) {
  if (!isRecord(value) || value.format !== "chunked-v1") invalidSnapshot("chunked manifest format is invalid");
  if (!DATABASE_EPOCH_PATTERN.test(value.database_epoch ?? "")) invalidSnapshot("database epoch is invalid");
  if (!positiveSafeInteger(value.state_version) || !positiveSafeInteger(value.reported_at)
      || typeof value.library_revision !== "string" || value.library_revision.length === 0
      || value.library_revision.length > 512 || /[\u0000-\u001f\u007f]/u.test(value.library_revision)) {
    invalidSnapshot("snapshot revision metadata is invalid");
  }
  if (value.synced !== true || typeof value.stale !== "boolean"
      || !positiveSafeInteger(value.last_seen) || !Array.isArray(value.pending_requests)) {
    invalidSnapshot("snapshot availability metadata is invalid");
  }
  const library = validatePartitionDescriptor(value.library, "library");
  const state = validatePartitionDescriptor(value.state, "state");
  const totalBytes = library.bytes + state.bytes;
  const totalChunks = library.chunks.length + state.chunks.length;
  if (totalBytes > MAX_TOTAL_BYTES) invalidSnapshot("combined snapshot exceeds the byte limit");
  if (totalChunks > MAX_CHUNKS) invalidSnapshot("snapshot has too many chunks");
  return {
    databaseEpoch: value.database_epoch,
    stateVersion: value.state_version,
    reportedAt: value.reported_at,
    libraryRevision: value.library_revision,
    library,
    state,
    synced: value.synced,
    lastSeen: value.last_seen,
    stale: value.stale,
    pendingRequests: value.pending_requests,
  };
}

function bytesToHex(bytes) {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function isAuthorizationFailure(error) {
  return error?.status === 401 || error?.status === 403;
}

async function digestSha256(bytes) {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle || typeof subtle.digest !== "function") {
    throw new Error("Secure snapshot verification is unavailable in this browser");
  }
  return bytesToHex(new Uint8Array(await subtle.digest("SHA-256", bytes)));
}

async function fetchChunk(fetchJson, descriptor, databaseEpoch) {
  const response = await fetchJson(`/snapshot/chunks/${descriptor.sha256}`, {
    "X-Cloud-Database-Epoch": databaseEpoch,
  });
  if (!isRecord(response) || response.sha256 !== descriptor.sha256 || typeof response.data !== "string") {
    invalidSnapshot("chunk response does not match the manifest");
  }
  const bytes = new TextEncoder().encode(response.data);
  if (bytes.byteLength !== descriptor.bytes || bytes.byteLength > MAX_CHUNK_BYTES) {
    invalidSnapshot("chunk byte count does not match the manifest");
  }
  if (await digestSha256(bytes) !== descriptor.sha256) invalidSnapshot("chunk hash does not match the manifest");
  return response.data;
}

function parsePartition(chunks, descriptor) {
  const json = chunks.join("");
  if (new TextEncoder().encode(json).byteLength !== descriptor.bytes) {
    invalidSnapshot("partition byte count does not match its chunks");
  }
  return JSON.parse(json);
}

function chunkCacheKey(databaseEpoch, sha256) {
  return `${databaseEpoch}:${sha256}`;
}

function createChunkCache() {
  const entries = new Map();
  let bytes = 0;
  let databaseEpoch = null;

  function remove(key) {
    const entry = entries.get(key);
    if (!entry) return;
    entries.delete(key);
    bytes -= entry.bytes;
  }

  function clear() {
    entries.clear();
    bytes = 0;
    databaseEpoch = null;
  }

  function retainManifest(manifest) {
    if (databaseEpoch !== manifest.databaseEpoch) {
      clear();
      databaseEpoch = manifest.databaseEpoch;
    }
    const refs = new Set([
      ...manifest.library.chunks.map((chunk) => chunkCacheKey(databaseEpoch, chunk.sha256)),
      ...manifest.state.chunks.map((chunk) => chunkCacheKey(databaseEpoch, chunk.sha256)),
    ]);
    for (const key of entries.keys()) {
      if (!refs.has(key)) remove(key);
    }
  }

  function get(key, expectedBytes) {
    const entry = entries.get(key);
    if (!entry) return undefined;
    if (entry.bytes !== expectedBytes) {
      remove(key);
      invalidSnapshot("cached chunk byte count does not match the manifest");
    }
    entries.delete(key);
    entries.set(key, entry);
    return entry.data;
  }

  function set(key, data, entryBytes) {
    remove(key);
    entries.set(key, { data, bytes: entryBytes });
    bytes += entryBytes;
    while (entries.size > MAX_CHUNKS || bytes > MAX_TOTAL_BYTES) {
      remove(entries.keys().next().value);
    }
  }

  return { clear, get, retainManifest, set };
}

async function fetchPartitions(fetchJson, manifest, cache) {
  const values = new Map();
  const jobsByKey = new Map();
  for (const chunk of [...manifest.library.chunks, ...manifest.state.chunks]) {
    const key = chunkCacheKey(manifest.databaseEpoch, chunk.sha256);
    const existing = jobsByKey.get(key);
    if (existing && existing.chunk.bytes !== chunk.bytes) {
      invalidSnapshot("duplicate chunk byte counts do not match");
    }
    if (!existing) jobsByKey.set(key, { key, chunk });
  }
  const jobs = [];
  for (const job of jobsByKey.values()) {
    const cached = cache?.get(job.key, job.chunk.bytes);
    if (cached === undefined) jobs.push(job);
    else values.set(job.key, cached);
  }
  let nextIndex = 0;
  let failed = false;
  let failure;
  const workerCount = Math.min(CHUNK_CONCURRENCY, jobs.length);
  await Promise.all(Array.from({ length: workerCount }, async () => {
    while (!failed) {
      const job = jobs[nextIndex];
      nextIndex += 1;
      if (!job) return;
      try {
        const data = await fetchChunk(fetchJson, job.chunk, manifest.databaseEpoch);
        cache?.set(job.key, data, job.chunk.bytes);
        values.set(job.key, data);
      } catch (error) {
        if (!failed || isAuthorizationFailure(error) && !isAuthorizationFailure(failure)) failure = error;
        failed = true;
        return;
      }
    }
  }));
  if (failed) throw failure;
  return [
    parsePartition(manifest.library.chunks.map((chunk) => values.get(chunkCacheKey(manifest.databaseEpoch, chunk.sha256))), manifest.library),
    parsePartition(manifest.state.chunks.map((chunk) => values.get(chunkCacheKey(manifest.databaseEpoch, chunk.sha256))), manifest.state),
  ];
}

function validateJsonComplexity(value) {
  let nodes = 0;
  const visit = (current, depth) => {
    nodes += 1;
    if (nodes > MAX_JSON_NODES || depth > MAX_JSON_DEPTH) {
      invalidSnapshot("JSON structure exceeds the complexity limit");
    }
    if (Array.isArray(current)) {
      for (const item of current) visit(item, depth + 1);
    } else if (isRecord(current)) {
      for (const key of Object.keys(current)) visit(current[key], depth + 1);
    }
  };
  visit(value, 0);
}

function validateAssembledShape(library, state) {
  if (!isRecord(library) || typeof library.revision !== "string"
      || !Number.isSafeInteger(library.scanned_at) || library.scanned_at < 0
      || !Array.isArray(library.items) || !Array.isArray(library.categories)
      || !library.categories.every((category) => typeof category === "string")
      || !Array.isArray(library.warnings) || !library.warnings.every((warning) => typeof warning === "string")) {
    invalidSnapshot("library partition has an invalid shape");
  }
  if (!isRecord(state) || Object.hasOwn(state, "library") || Object.hasOwn(state, "state_version")
      || Object.hasOwn(state, "reported_at") || !Array.isArray(state.previews)
      || !Array.isArray(state.operations) || !Array.isArray(state.tasks)
      || !isRecord(state.settings) || typeof state.settings.download_dir !== "string") {
    invalidSnapshot("state partition has an invalid shape");
  }
  if (state.revision !== undefined && typeof state.revision !== "string") {
    invalidSnapshot("state revision has an invalid shape");
  }
}

async function loadOwnerSnapshotWithCache(fetchJson, cache) {
  const response = await fetchJson("/state");
  if (!isRecord(response) || typeof response.format !== "string" || !response.format.startsWith("chunked-")) {
    cache?.clear();
    return response;
  }
  if (response.format !== "chunked-v1") invalidSnapshot("chunked manifest format is unsupported");

  const manifest = validateManifest(response);
  validateJsonComplexity(response);
  cache?.retainManifest(manifest);
  const [library, state] = await fetchPartitions(fetchJson, manifest, cache);
  validateJsonComplexity(library);
  validateJsonComplexity(state);
  validateAssembledShape(library, state);
  if (library.revision !== manifest.libraryRevision) invalidSnapshot("library revision does not match the manifest");

  const snapshot = {
    ...state,
    synced: manifest.synced,
    state_version: manifest.stateVersion,
    reported_at: manifest.reportedAt,
    last_seen: manifest.lastSeen,
    stale: manifest.stale,
    library,
    pending_requests: manifest.pendingRequests,
  };
  return snapshot;
}

/**
 * Load a legacy owner snapshot or assemble a chunked-v1 snapshot. The callback
 * must perform the existing authenticated, no-store JSON request for the path
 * and optional headers it receives.
 */
export async function loadOwnerSnapshot(fetchJson) {
  return loadOwnerSnapshotWithCache(fetchJson, null);
}

/**
 * Create a page-local snapshot loader that reuses verified raw chunks in memory.
 * Each call still fetches the authenticated manifest and parses fresh objects.
 */
export function createOwnerSnapshotLoader() {
  const cache = createChunkCache();
  let pending = Promise.resolve();

  return (fetchJson) => {
    const result = pending.then(async () => {
      try {
        return await loadOwnerSnapshotWithCache(fetchJson, cache);
      } catch (error) {
        cache.clear();
        throw error;
      }
    });
    pending = result.catch(() => undefined);
    return result;
  };
}
