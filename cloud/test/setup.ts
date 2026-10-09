import { applyD1Migrations, reset } from "cloudflare:test";
import { env } from "cloudflare:workers";
import type { D1Migration } from "@cloudflare/vitest-plugin";
import { beforeEach } from "vitest";

const testEnv = env as typeof env & { TEST_MIGRATIONS: D1Migration[] };

beforeEach(async () => {
  await reset();
  await applyD1Migrations(testEnv.DB, testEnv.TEST_MIGRATIONS);
});
