import { defineConfig } from "vitest/config";

try {
  process.loadEnvFile("../.env");
} catch {
  // no .env: rely on the real environment (CI)
}

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // Integration tests share one Postgres and MinIO
    fileParallelism: false,
    testTimeout: 20_000,
  },
});
