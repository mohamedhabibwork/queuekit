import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const root = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "@mohamedhabibwork/core/testing": `${root}packages/core/src/testing.ts`,
      "@mohamedhabibwork/core": `${root}packages/core/src/index.ts`,
      "@mohamedhabibwork/memory": `${root}packages/memory/src/index.ts`,
    },
  },
  test: {
    environment: "node",
    include: ["packages/*/test/**/*.test.ts"],
    typecheck: {
      enabled: true,
      include: ["packages/*/test/**/*.test-d.ts"],
      tsconfig: `${root}tsconfig.json`,
    },
  },
});
