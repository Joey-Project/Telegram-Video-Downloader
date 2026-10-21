import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.ui.ts"],
    environment: "happy-dom",
    environmentOptions: {
      happyDOM: {
        url: "http://localhost/file/",
        settings: {
          disableJavaScriptFileLoading: true,
          enableJavaScriptEvaluation: false,
          enableCSSFileLoading: false,
          enableImageFileLoading: false,
          enableIframePageLoading: false,
          disableComputedStyleRendering: true,
        },
      },
    },
    testTimeout: 15_000,
  },
});
