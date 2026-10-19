export const MAX_TARGET_RELATIVE_DIR_BYTES: 1024;
export function validateTargetRelativeDirectory(value: unknown): {
  directory: string;
  error: string | null;
};
