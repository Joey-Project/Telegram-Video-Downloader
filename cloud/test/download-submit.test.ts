import { describe, expect, it } from "vitest";
import { buildDownloadSubmitPayload, isBoundDownloadLibraryOption } from "../public/file/download-submit.js";

describe("download submission binding", () => {
  const rootBinding = `v1:${"a".repeat(64)}`;

  it("builds the request from the selected rendered option and preserves an explicit blank category", () => {
    const selectedOption = { value: "archive", dataset: { rootBinding } };
    expect(buildDownloadSubmitPayload({
      url: " https://www.bilibili.com/video/BV1example ",
      selectedOption,
      category: "",
    })).toEqual({
      url: "https://www.bilibili.com/video/BV1example",
      library_id: "archive",
      root_binding: rootBinding,
      category: "",
    });
  });

  it("keeps an omitted category omitted", () => {
    const payload = buildDownloadSubmitPayload({
      url: "https://www.bilibili.com/video/BV1example",
      selectedOption: { value: "archive", dataset: { rootBinding } },
    });
    expect(payload).toEqual({
      url: "https://www.bilibili.com/video/BV1example",
      library_id: "archive",
      root_binding: rootBinding,
    });
    expect(payload).not.toHaveProperty("category");
  });

  it.each([
    null,
    { value: "archive", dataset: {} },
    { value: "archive", dataset: { rootBinding: `v1:${"A".repeat(64)}` } },
    { value: "archive", dataset: { rootBinding: `v1:${"a".repeat(63)}` } },
    { value: "", dataset: { rootBinding } },
    { value: " archive", dataset: { rootBinding } },
  ])("fails closed when the rendered option has no valid registration identity: %j", (selectedOption) => {
    expect(isBoundDownloadLibraryOption(selectedOption)).toBe(false);
    expect(buildDownloadSubmitPayload({ url: "https://example.com/video", selectedOption })).toBeNull();
  });
});
