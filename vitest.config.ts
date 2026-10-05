import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    fileParallelism: false,
    testTimeout: 60000,
    include: ["**/*.test.ts", "**/*.spec.ts"],
  },
});
