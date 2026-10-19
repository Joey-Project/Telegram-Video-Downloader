export const MAX_TARGET_RELATIVE_DIR_BYTES: 1024;
export const MAX_TARGET_RELATIVE_DIR_COMPONENT_BYTES: 255;
export function validateTargetRelativeDirectory(value: unknown): {
  directory: string;
  error: string | null;
};
