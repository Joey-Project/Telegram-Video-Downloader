export interface BoundDownloadLibraryOption {
  value?: unknown;
  dataset?: { rootBinding?: unknown } | null;
}

export interface DownloadSubmitPayload {
  url: string;
  library_id: string;
  root_binding: string;
  category?: string;
}

export function isBoundDownloadLibraryOption(option: unknown): boolean;
export function buildDownloadSubmitPayload(options?: {
  url?: unknown;
  selectedOption?: BoundDownloadLibraryOption | null;
  category?: unknown;
}): DownloadSubmitPayload | null;
