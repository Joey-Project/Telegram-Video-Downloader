export function submitLegacyFileImport(
  file: Pick<File, "size" | "text">,
  submit: (text: string) => void,
  reject: (message: string) => void,
  maxBytes: number,
): Promise<boolean>;
