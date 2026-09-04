import type { ProviderTypes } from "@mohamedhabibwork/core";
import type { Clock, DurationInput, QueueSize } from "@mohamedhabibwork/core";

/** Options accepted by the in-memory provider factory. */
export interface MemoryProviderOptions {
  /** Injectable clock — pass a FakeClock for deterministic delay/retry tests. */
  clock?: Clock | undefined;
  /** Default visibility lease applied to received messages. Default "60s". */
  defaultVisibilityTimeout?: DurationInput | undefined;
  /** Kept for ProviderConfig symmetry; the memory adapter never blocks connect. */
  lazy?: boolean | undefined;
}

/** Native send options for the memory provider: higher priority is delivered first. */
export interface MemorySendNativeOptions {
  priority?: number | undefined;
}

/** Native delivery handle: identifies one specific delivery of a message. */
export interface MemoryReceipt {
  messageId: string;
  receiptId: string;
  queuePhysical: string;
}

export interface MemorySendNativeResult {
  id: string;
  enqueuedAt: number;
  deduplicated: boolean;
}

/**
 * Test/inspection surface exposed via `queue.native()`. Deliberately small:
 * introspection only, never needed for normal processing.
 */
export interface MemoryQueueInspector {
  size(queue: string): Promise<QueueSize>;
  /** All envelope ids currently known for a queue, in insertion order. */
  envelopeIds(queue: string): string[];
  /** Raw access for assertions in tests. */
  snapshot(queue: string): Array<{
    messageId: string;
    attempt: number;
    state: "pending" | "inflight";
    availableAt: number;
    priority: number;
  }>;
}

/**
 * Type registry of the memory provider. Members that the provider genuinely
 * does not have are `never`, so `native` options are unusable rather than
 * lying with a broad type.
 */
export interface MemoryTypes extends ProviderTypes {
  connection: MemoryProviderOptions;
  queue: never;
  send: MemorySendNativeOptions;
  receive: never;
  worker: never;
  nack: never;
  schedule: never;
  messageId: string;
  nativeMessage: MemoryReceipt;
  nativeClient: MemoryQueueInspector;
  nativeResult: MemorySendNativeResult;
}
