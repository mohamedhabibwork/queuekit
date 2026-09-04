import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const root = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "@queue-kit/core/testing": `${root}packages/core/src/testing.ts`,
      "@queue-kit/core": `${root}packages/core/src/index.ts`,
      "@queue-kit/memory": `${root}packages/memory/src/index.ts`,
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
