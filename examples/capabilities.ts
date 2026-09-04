/**
 * Capability discovery: the same job graph on a provider that lacks a
 * portable feature fails loudly and early — never silently approximates.
 *
 * Run with: bun examples/capabilities.ts
 */
import { createQueue, UnsupportedCapabilityError } from "@mohamedhabibwork/core";
import { memory } from "@mohamedhabibwork/memory";

const queue = createQueue({ name: "reports", provider: memory() });

console.log("provider:", queue.describe().provider);
console.log("supports delay:", queue.supports("delayedDelivery"));
console.log("supports fifo:", queue.supports("fifo"));

// Portable features work when declared:
await queue.send("report.build", { day: "2026-09-04" }, { delay: "1h" });

// Providers without a feature throw UnsupportedCapabilityError instead of
// guessing. (The memory provider has no cron scheduler — an explicit error
// beats a fake one.)
try {
  await queue.schedule("report.build", {}, { cron: "0 6 * * *" });
} catch (error) {
  console.log(
    "cron refused:",
    error instanceof UnsupportedCapabilityError ? error.message : error,
  );
}
