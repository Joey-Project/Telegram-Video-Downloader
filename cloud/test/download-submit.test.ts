import { describe, expect, it } from "vitest";
import { buildDownloadSubmitPayload, isBoundDownloadLibraryOption } from "../public/file/download-submit.js";

describe("download submission binding", () => {
  const rootBindingV1 = `v1:${"a".repeat(64)}`;
  const rootBindingV2 = `v2:${"b".repeat(64)}`;

  it.each([rootBindingV1, rootBindingV2])("builds the request from a selected option using %s", (rootBinding) => {
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
      selectedOption: { value: "archive", dataset: { rootBinding: rootBindingV2 } },
    });
    expect(payload).toEqual({
      url: "https://www.bilibili.com/video/BV1example",
      library_id: "archive",
      root_binding: rootBindingV2,
    });
    expect(payload).not.toHaveProperty("category");
  });

  it.each([
    null,
    { value: "archive", dataset: {} },
    { value: "archive", dataset: { rootBinding: `v1:${"A".repeat(64)}` } },
    { value: "archive", dataset: { rootBinding: `v1:${"a".repeat(63)}` } },
    { value: "archive", dataset: { rootBinding: `v3:${"a".repeat(64)}` } },
    { value: "archive", dataset: { rootBinding: `${rootBindingV2}\n` } },
    { value: "", dataset: { rootBinding: rootBindingV1 } },
    { value: " archive", dataset: { rootBinding: rootBindingV1 } },
  ])("fails closed when the rendered option has no valid registration identity: %j", (selectedOption) => {
    expect(isBoundDownloadLibraryOption(selectedOption)).toBe(false);
    expect(buildDownloadSubmitPayload({ url: "https://example.com/video", selectedOption })).toBeNull();
  });
});
