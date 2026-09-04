import { defineConfig } from "tsup";

// Declarations (.d.ts) are emitted by `tsc -p tsconfig.build.json` from
// the build script instead of tsup's bundled rollup-plugin-dts, whose
// tsup-embedded copy crashes under TypeScript 7 (ts.sys is no longer
// exposed).
const shared = {
  format: ["esm"],
  dts: false,
  sourcemap: true,
  target: "es2022",
  splitting: true,
  treeshake: true,
  external: ["@queue-kit/core"],
};

export default defineConfig([
  {
    ...shared,
    entry: { index: "src/index.ts" },
    clean: true,
  },
  {
    ...shared,
    entry: { testing: "src/testing-entry.ts" },
  },
]);
