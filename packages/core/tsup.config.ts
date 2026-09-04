import { defineConfig } from "tsup";

export default defineConfig([
  {
    entry: { index: "src/index.ts" },
    format: ["esm"],
    dts: true,
    sourcemap: true,
    clean: true,
    target: "es2022",
    splitting: true,
    treeshake: true,
  },
  {
    entry: { testing: "src/testing-entry.ts" },
    format: ["esm"],
    dts: true,
    sourcemap: true,
    target: "es2022",
    splitting: true,
    treeshake: true,
  },
]);
