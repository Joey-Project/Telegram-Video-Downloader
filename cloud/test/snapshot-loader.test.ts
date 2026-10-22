import { webcrypto } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadOwnerSnapshot } from "../public/file/snapshot-loader.js";
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
