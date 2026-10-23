export type SnapshotUpdatesStatus = "connecting" | "live" | "fallback" | "offline" | "paused" | "expired";

export interface SnapshotUpdatesRefreshOptions {
  quiet: boolean;
}

export interface SnapshotUpdatesEnvironment {
  window?: Window & typeof globalThis;
  document?: Document;
  navigator?: Navigator;
  location?: Location;
  WebSocket?: typeof WebSocket;
  AbortController?: typeof AbortController;
  telegram?: {
    onEvent?: (event: "activated" | "deactivated", handler: () => void) => void;
    offEvent?: (event: "activated" | "deactivated", handler: () => void) => void;
  };
  now?: () => number;
  random?: () => number;
  setTimeout?: typeof globalThis.setTimeout;
  clearTimeout?: typeof globalThis.clearTimeout;
}

export interface SnapshotUpdatesOptions {
  api(path: string, options: { method: "POST"; signal?: AbortSignal }): Promise<unknown>;
  refresh(options: SnapshotUpdatesRefreshOptions): Promise<unknown> | unknown;
  onAuthorizationExpired?: () => void;
  onStatus?: (status: SnapshotUpdatesStatus) => void;
  environment?: SnapshotUpdatesEnvironment;
}

export interface SnapshotUpdatesController {
  start(): void;
  stop(): void;
  refresh(options?: Partial<SnapshotUpdatesRefreshOptions>): Promise<void>;
}

export function createSnapshotUpdates(options: SnapshotUpdatesOptions): SnapshotUpdatesController;
