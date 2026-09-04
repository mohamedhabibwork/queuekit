import { parseDuration, systemClock, type Clock, type QueueProvider } from "@mohamedhabibwork/core";
import { MemoryBroker } from "./broker";
import { MemoryQueueAdapter } from "./adapter";
import type { MemoryProviderOptions, MemoryTypes } from "./types";

export type { MemoryProviderOptions, MemoryTypes, MemoryReceipt, MemorySendNativeOptions, MemoryQueueInspector, MemorySendNativeResult } from "./types";
export { MemoryBroker } from "./broker";
export { MemoryQueueAdapter } from "./adapter";

/**
 * In-memory Queue Kit provider for unit tests, examples and local
 * development. Supports delay, priority, visibility timeouts, ack/nack,
 * retries (queue-kit strategy), dead-lettering, pause/resume and
 * idempotency-key deduplication — but no durability. Never use in
 * production.
 *
 * ```ts
 * import { createQueue } from "@mohamedhabibwork/core";
 * import { memory } from "@mohamedhabibwork/memory";
 *
 * const queue = createQueue({ name: "emails", provider: memory() });
 * ```
 */
export function memory(options: MemoryProviderOptions = {}): QueueProvider<MemoryTypes> {
  const clock: Clock = options.clock ?? systemClock;
  const defaultVisibilityTimeoutMs =
        options.defaultVisibilityTimeout === undefined ? 60_000 : parseDuration(options.defaultVisibilityTimeout);
  const broker = new MemoryBroker(clock, defaultVisibilityTimeoutMs);
  const adapter = new MemoryQueueAdapter(broker);

  return {
    id: "memory",
    capabilities: adapter.capabilities,
    adapter,
    connection: options,
    lazy: true,
    resolvePhysicalName: (logicalName) => logicalName,
  };
}
