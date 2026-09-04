import { defineConfig } from "tsup";

// Declarations (.d.ts) are emitted by `tsc -p tsconfig.build.json` from
// the build script instead of tsup's bundled rollup-plugin-dts, whose
// tsup-embedded copy crashes under TypeScript 7 (ts.sys is no longer
// exposed).
export default defineConfig({
  entry: { index: "src/index.ts" },
  format: ["esm"],
  dts: false,
  sourcemap: true,
  clean: true,
  target: "es2022",
  external: ["@queue-kit/core"],
});
