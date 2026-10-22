import { webcrypto } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createOwnerSnapshotLoader, loadOwnerSnapshot } from "../public/file/snapshot-loader.js";
import { makeChunkedSnapshot, TEST_DATABASE_EPOCH } from "./snapshot-loader-fixtures.js";

const ownerSnapshot = {
  synced: true,
  state_version: 41,
  revision: "state-revision",
  reported_at: 1_800_000_010,
  last_seen: 1_800_000_000,
  stale: false,
  settings: { download_dir: "/srv/media" },
  library: {
    revision: "library-revision",
    scanned_at: 1_799_999_000,
    items: [{ id: "one", relative_path: "Films/One.mkv" }],
    categories: ["Films"],
    warnings: [],
    libraries: [{ id: "default", availability: "available", available: true, is_default: true }],
  },
  previews: [],
  operations: [],
  tasks: [],
  pending_requests: [{ seq: 7, request_id: "request-7" }],
};

describe("owner snapshot loader", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("passes legacy full snapshots through without fetching chunks", async () => {
    const fetchJson = vi.fn(async () => ownerSnapshot);

    await expect(loadOwnerSnapshot(fetchJson)).resolves.toBe(ownerSnapshot);
    expect(fetchJson).toHaveBeenCalledExactlyOnceWith("/state");
  });

  it("assembles library and state partitions with owner epoch headers and bounded concurrency", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const largeSnapshot = {
      ...ownerSnapshot,
      library: {
        ...ownerSnapshot.library,
        items: Array.from({ length: 12 }, (_, index) => ({ id: `large-${index}`, title: "x".repeat(4_000) })),
      },
    };
    const { manifest, chunks } = await makeChunkedSnapshot(largeSnapshot, { chunkBytes: 4_000 });
    let inFlight = 0;
    let maximumInFlight = 0;
    const fetchJson = vi.fn(async (path: string, headers?: Record<string, string>) => {
      if (path === "/state") return manifest;
      expect(path).toMatch(/^\/snapshot\/chunks\/[0-9a-f]{64}$/u);
      expect(headers).toEqual({ "X-Cloud-Database-Epoch": TEST_DATABASE_EPOCH });
      inFlight += 1;
      maximumInFlight = Math.max(maximumInFlight, inFlight);
      await new Promise((resolve) => setTimeout(resolve, 1));
      inFlight -= 1;
      const hash = path.split("/").at(-1)!;
      return { sha256: hash, data: chunks.get(hash) };
    });

    const assembled = await loadOwnerSnapshot(fetchJson) as typeof ownerSnapshot;

    expect(assembled).toMatchObject({
      synced: true,
      state_version: 41,
      reported_at: 1_800_000_010,
      last_seen: 1_800_000_000,
      stale: false,
      pending_requests: [{ seq: 7, request_id: "request-7" }],
      library: { revision: "library-revision", items: expect.arrayContaining([{ id: "large-0", title: "x".repeat(4_000) }]) },
      settings: { download_dir: "/srv/media" },
    });
    expect(fetchJson.mock.calls[0]?.[0]).toBe("/state");
    expect(maximumInFlight).toBeGreaterThan(1);
    expect(maximumInFlight).toBeLessThanOrEqual(4);
  });

  it("reuses verified chunks for an unchanged manifest and parses fresh snapshot objects", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);
    const loadSnapshot = createOwnerSnapshotLoader();
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return manifest;
      const hash = path.split("/").at(-1)!;
      return { sha256: hash, data: chunks.get(hash) };
    });

    const first = await loadSnapshot(fetchJson) as typeof ownerSnapshot;
    first.library.items[0]!.id = "mutated-in-app";
    const second = await loadSnapshot(fetchJson) as typeof ownerSnapshot;

    expect(fetchJson.mock.calls.filter(([path]) => path === "/state")).toHaveLength(2);
    expect(fetchJson.mock.calls.filter(([path]) => path !== "/state")).toHaveLength(
      new Set([...manifest.library.chunks, ...manifest.state.chunks].map((chunk: { sha256: string }) => chunk.sha256)).size,
    );
    expect(second).not.toBe(first);
    expect(second.library).not.toBe(first.library);
    expect(second.library.items[0]?.id).toBe("one");
  });

  it("keeps chunk caches private to each loader instance", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);
    const loaders = [createOwnerSnapshotLoader(), createOwnerSnapshotLoader()];
    let chunkRequests = 0;
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return manifest;
      chunkRequests += 1;
      const hash = path.split("/").at(-1)!;
      return { sha256: hash, data: chunks.get(hash) };
    });

    await loaders[0]!(fetchJson);
    await loaders[1]!(fetchJson);

    expect(chunkRequests).toBe(2 * new Set([...manifest.library.chunks, ...manifest.state.chunks].map((chunk: { sha256: string }) => chunk.sha256)).size);
  });

  it("reuses unchanged library chunks when only the state partition changes", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const changedSnapshot = {
      ...ownerSnapshot,
      state_version: ownerSnapshot.state_version + 1,
      settings: { download_dir: "/srv/new-media" },
    };
    const first = await makeChunkedSnapshot(ownerSnapshot);
    const second = await makeChunkedSnapshot(changedSnapshot);
    const responses = [first, second];
    let manifestIndex = 0;
    const chunkCalls = new Map<string, number>();
    const loadSnapshot = createOwnerSnapshotLoader();
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return responses[manifestIndex++]!.manifest;
      const hash = path.split("/").at(-1)!;
      chunkCalls.set(hash, (chunkCalls.get(hash) ?? 0) + 1);
      const chunk = second.chunks.get(hash) ?? first.chunks.get(hash);
      return { sha256: hash, data: chunk };
    });

    await loadSnapshot(fetchJson);
    const changed = await loadSnapshot(fetchJson) as typeof ownerSnapshot;

    expect(changed.settings.download_dir).toBe("/srv/new-media");
    for (const chunk of first.manifest.library.chunks) expect(chunkCalls.get(chunk.sha256)).toBe(1);
    for (const chunk of second.manifest.state.chunks) expect(chunkCalls.get(chunk.sha256)).toBe(1);
  });

  it("refetches matching content after the database epoch changes", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const first = await makeChunkedSnapshot(ownerSnapshot);
    const second = await makeChunkedSnapshot(ownerSnapshot, { databaseEpoch: "c".repeat(32) });
    const responses = [first, second];
    let manifestIndex = 0;
    const chunkCalls = new Map<string, number>();
    const loadSnapshot = createOwnerSnapshotLoader();
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return responses[manifestIndex++]!.manifest;
      const hash = path.split("/").at(-1)!;
      chunkCalls.set(hash, (chunkCalls.get(hash) ?? 0) + 1);
      return { sha256: hash, data: first.chunks.get(hash) };
    });

    await loadSnapshot(fetchJson);
    await loadSnapshot(fetchJson);

    for (const chunk of first.manifest.library.chunks) expect(chunkCalls.get(chunk.sha256)).toBe(2);
    for (const chunk of first.manifest.state.chunks) expect(chunkCalls.get(chunk.sha256)).toBe(2);
  });

  it("clears cached chunks after an authorization failure", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const changedSnapshot = {
      ...ownerSnapshot,
      state_version: ownerSnapshot.state_version + 1,
      settings: { download_dir: "/srv/new-media" },
    };
    const first = await makeChunkedSnapshot(ownerSnapshot);
    const second = await makeChunkedSnapshot(changedSnapshot);
    const responses = [first, second, second];
    let manifestIndex = 0;
    let rejectChunk = false;
    const denied = Object.assign(new Error("Owner verification required"), { status: 403 });
    const chunkCalls = new Map<string, number>();
    const loadSnapshot = createOwnerSnapshotLoader();
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return responses[manifestIndex++]!.manifest;
      const hash = path.split("/").at(-1)!;
      chunkCalls.set(hash, (chunkCalls.get(hash) ?? 0) + 1);
      if (rejectChunk) throw denied;
      return { sha256: hash, data: second.chunks.get(hash) ?? first.chunks.get(hash) };
    });

    await loadSnapshot(fetchJson);
    rejectChunk = true;
    await expect(loadSnapshot(fetchJson)).rejects.toBe(denied);
    rejectChunk = false;
    await loadSnapshot(fetchJson);

    for (const chunk of second.manifest.library.chunks) expect(chunkCalls.get(chunk.sha256)).toBe(2);
  });

  it("does not retain any chunk from a failed or corrupt assembly", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);
    const corruptHash = manifest.library.chunks[0]!.sha256;
    let corrupt = true;
    const chunkCalls = new Map<string, number>();
    const loadSnapshot = createOwnerSnapshotLoader();
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return manifest;
      const hash = path.split("/").at(-1)!;
      chunkCalls.set(hash, (chunkCalls.get(hash) ?? 0) + 1);
      const data = chunks.get(hash)!;
      return { sha256: hash, data: corrupt && hash === corruptHash ? `${data} ` : data };
    });

    await expect(loadSnapshot(fetchJson)).rejects.toThrow(/chunk byte count|chunk hash/u);
    corrupt = false;
    await loadSnapshot(fetchJson);

    for (const chunk of [...manifest.library.chunks, ...manifest.state.chunks]) expect(chunkCalls.get(chunk.sha256)).toBe(2);
  });

  it("evicts chunks outside the current manifest references", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const changedSnapshot = {
      ...ownerSnapshot,
      state_version: ownerSnapshot.state_version + 1,
      settings: { download_dir: "/srv/other-media" },
      library: { ...ownerSnapshot.library, revision: "library-revision-b", items: [{ id: "two", relative_path: "Films/Two.mkv" }] },
    };
    const first = await makeChunkedSnapshot(ownerSnapshot);
    const second = await makeChunkedSnapshot(changedSnapshot);
    const responses = [first, second, first];
    let manifestIndex = 0;
    const chunkCalls = new Map<string, number>();
    const loadSnapshot = createOwnerSnapshotLoader();
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return responses[manifestIndex++]!.manifest;
      const hash = path.split("/").at(-1)!;
      chunkCalls.set(hash, (chunkCalls.get(hash) ?? 0) + 1);
      return { sha256: hash, data: first.chunks.get(hash) ?? second.chunks.get(hash) };
    });

    await loadSnapshot(fetchJson);
    await loadSnapshot(fetchJson);
    await loadSnapshot(fetchJson);

    for (const chunk of [...first.manifest.library.chunks, ...first.manifest.state.chunks]) expect(chunkCalls.get(chunk.sha256)).toBe(2);
    for (const chunk of [...second.manifest.library.chunks, ...second.manifest.state.chunks]) expect(chunkCalls.get(chunk.sha256)).toBe(1);
  });

  it("clears cached chunks when the endpoint returns a legacy snapshot", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);
    const responses = [manifest, ownerSnapshot, manifest];
    let manifestIndex = 0;
    let chunkRequests = 0;
    const loadSnapshot = createOwnerSnapshotLoader();
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return responses[manifestIndex++]!;
      chunkRequests += 1;
      const hash = path.split("/").at(-1)!;
      return { sha256: hash, data: chunks.get(hash) };
    });

    await loadSnapshot(fetchJson);
    await expect(loadSnapshot(fetchJson)).resolves.toBe(ownerSnapshot);
    await loadSnapshot(fetchJson);

    expect(chunkRequests).toBe(2 * new Set([...manifest.library.chunks, ...manifest.state.chunks].map((chunk: { sha256: string }) => chunk.sha256)).size);
  });

  it("applies depth limits to each stored partition without adding the combined response envelope", async () => {
    vi.stubGlobal("crypto", webcrypto);
    let nested: unknown = "leaf";
    for (let level = 0; level < 47; level += 1) nested = { child: nested };
    const snapshot = {
      ...ownerSnapshot,
      library: { ...ownerSnapshot.library, extension: nested },
    };
    const { manifest, chunks } = await makeChunkedSnapshot(snapshot);
    const fetchJson = async (path: string) => {
      if (path === "/state") return manifest;
      const hash = path.split("/").at(-1)!;
      return { sha256: hash, data: chunks.get(hash) };
    };

    await expect(loadOwnerSnapshot(fetchJson)).resolves.toMatchObject({ library: { extension: nested } });
  });

  it("rejects a corrupt chunk before returning any assembled snapshot", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const { manifest, chunks } = await makeChunkedSnapshot(ownerSnapshot);
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return manifest;
      const hash = path.split("/").at(-1)!;
      return { sha256: hash, data: `${chunks.get(hash)} ` };
    });

    await expect(loadOwnerSnapshot(fetchJson)).rejects.toThrow(/chunk byte count|chunk hash/u);
  });

  it("propagates owner authorization failure from a chunk request", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const { manifest } = await makeChunkedSnapshot(ownerSnapshot);
    const denied = Object.assign(new Error("Owner verification required"), { status: 403 });
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return manifest;
      throw denied;
    });

    await expect(loadOwnerSnapshot(fetchJson)).rejects.toBe(denied);
  });

  it("preserves a racing chunk authorization failure over another chunk error", async () => {
    vi.stubGlobal("crypto", webcrypto);
    const largeSnapshot = {
      ...ownerSnapshot,
      library: {
        ...ownerSnapshot.library,
        items: Array.from({ length: 12 }, (_, index) => ({ id: `large-${index}`, title: "x".repeat(4_000) })),
      },
    };
    const { manifest } = await makeChunkedSnapshot(largeSnapshot, { chunkBytes: 4_000 });
    const denied = Object.assign(new Error("Owner verification required"), { status: 401 });
    let chunkRequest = 0;
    const fetchJson = vi.fn(async (path: string) => {
      if (path === "/state") return manifest;
      const request = chunkRequest;
      chunkRequest += 1;
      if (request === 0) throw new Error("A different in-flight chunk failed");
      if (request === 1) {
        await new Promise((resolve) => setTimeout(resolve, 1));
        throw denied;
      }
      return {};
    });

    await expect(loadOwnerSnapshot(fetchJson)).rejects.toBe(denied);
  });
});
