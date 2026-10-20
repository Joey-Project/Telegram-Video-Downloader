import { cloudflareTest, readD1Migrations } from "@cloudflare/vitest-plugin";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";
import { syntheticCredentials } from "./test/fixtures/synthetic-credentials.ts";

const migrationsPath = fileURLToPath(new URL("./migrations/", import.meta.url));

export default defineConfig({
  plugins: [cloudflareTest(async () => ({
    wrangler: { configPath: "./wrangler.jsonc" },
    miniflare: {
      bindings: {
        OWNER_USER_ID: "123456789",
        LOCAL_SHARED_SECRET: syntheticCredentials.localSharedSecret.value,
        WEBHOOK_SECRET: syntheticCredentials.webhookSecret.value,
        BOT_TOKEN: syntheticCredentials.botToken.value,
        TEST_MIGRATIONS: await readD1Migrations(migrationsPath),
      },
    },
  }))],
  test: {
    include: ["test/**/*.test.ts"],
    setupFiles: ["./test/setup.ts"],
    testTimeout: 15_000,
  },
});
