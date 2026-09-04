/**
 * A single capability declaration. Beyond a boolean, a capability can declare
 * how it is implemented (`native` vs `emulated`) and provider-specific
 * details such as maximum payload size.
 */
export interface Capability<TDetails = unknown> {
  supported: boolean;
  /** `native` = provided by the provider itself, `emulated` = implemented by the adapter/core. */
  mode?: "native" | "emulated" | undefined;
  details?: TDetails | undefined;
}

export interface SendCapabilityDetails {
  /** Maximum encoded payload size in bytes, when the provider enforces one. */
  maxBytes?: number;
  supportsBinary?: boolean;
}

export interface DelayCapabilityDetails {
  maxDelayMs?: number;
}

export interface BatchCapabilityDetails {
  maxItems?: number;
}

export interface FifoCapabilityDetails {
  /** Ordering is only guaranteed inside a group/partition, never globally. */
  requiresGroupId?: boolean;
}

export interface RetryCapabilityDetails {
  strategy: RetryStrategy;
}

export type RetryStrategy = "native" | "queue-kit" | "mixed" | "none";

export type DeadLetterStrategy = "provider-native" | "queue-kit" | "none";

/**
 * The full capability map. Providers fill this in; `supports()`/`assertCapability()`
 * consume it. Never silently ignore an unsupported portable option — the
 * capability map is the contract that makes that possible.
 */
export interface QueueCapabilities {
  send: Capability<SendCapabilityDetails>;
  batchSend: Capability<BatchCapabilityDetails>;
  receive: Capability;
  delayedDelivery: Capability<DelayCapabilityDetails>;
  scheduling: Capability;
  priorities: Capability;
  fifo: Capability<FifoCapabilityDetails>;
  deduplication: Capability;
  retries: Capability<RetryCapabilityDetails>;
  deadLetterQueue: Capability<{ strategy: DeadLetterStrategy }>;
  visibilityTimeout: Capability;
  acknowledgements: Capability;
  negativeAcknowledgements: Capability;
  rateLimit: Capability;
  pauseResume: Capability;
  concurrency: Capability;
  repeatableJobs: Capability;
  jobDependencies: Capability;
  transactionalPublish: Capability;
}

export type CapabilityName = keyof QueueCapabilities;

/** Start from an all-disabled map and layer provider support on top. */
export function createCapabilities(
  overrides: { [K in CapabilityName]?: Partial<QueueCapabilities[K]> & { supported?: boolean } } = {},
): QueueCapabilities {
  const capabilities: QueueCapabilities = {
    send: { supported: false },
    batchSend: { supported: false },
    receive: { supported: false },
    delayedDelivery: { supported: false },
    scheduling: { supported: false },
    priorities: { supported: false },
    fifo: { supported: false },
    deduplication: { supported: false },
    retries: { supported: false, details: { strategy: "none" } },
    deadLetterQueue: { supported: false, details: { strategy: "none" } },
    visibilityTimeout: { supported: false },
    acknowledgements: { supported: false },
    negativeAcknowledgements: { supported: false },
    rateLimit: { supported: false },
    pauseResume: { supported: false },
    concurrency: { supported: false },
    repeatableJobs: { supported: false },
    jobDependencies: { supported: false },
    transactionalPublish: { supported: false },
  };
  for (const key of CAPABILITY_NAMES) {
    const override = overrides[key];
    if (override !== undefined) {
      Object.assign(capabilities[key], override);
    }
  }
  return capabilities;
}

export const CAPABILITY_NAMES = [
  "send",
  "batchSend",
  "receive",
  "delayedDelivery",
  "scheduling",
  "priorities",
  "fifo",
  "deduplication",
  "retries",
  "deadLetterQueue",
  "visibilityTimeout",
  "acknowledgements",
  "negativeAcknowledgements",
  "rateLimit",
  "pauseResume",
  "concurrency",
  "repeatableJobs",
  "jobDependencies",
  "transactionalPublish",
] as const satisfies readonly CapabilityName[];
