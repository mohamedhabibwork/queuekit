import type { DurationInput } from "../duration";
import type { TraceContext } from "../envelope";
import type { JobRefLike } from "../job";

/**
 * Portable send options — only semantics that are consistent across
 * providers live here (see the feature mapping spec). Anything
 * provider-specific belongs in `native`, fully typed by the adapter.
 *
 * `delay` means: earliest time before which the provider should not make the
 * message available. Providers that cannot delay raise
 * `UnsupportedCapabilityError` — they never approximate silently.
 */
export interface CommonSendOptions {
  delay?: DurationInput | undefined;
  /** Application-level intent. Not a provider dedup guarantee (that is native). */
  idempotencyKey?: string | undefined;
  /** Flows into envelope metadata as `correlation-id` and to the adapter. */
  correlationId?: string | undefined;
  metadata?: Record<string, string> | undefined;
  trace?: TraceContext | undefined;
  /** Abort the publish attempt. */
  signal?: AbortSignal | undefined;
}

/** Provider-aware send options: portable options plus typed native options. */
export interface SendOptions<TNative = unknown> extends CommonSendOptions {
  /** Native options of the *selected* provider — inferred, never `unknown`. */
  native?: TNative | undefined;
}

/** Mutable view of send options handed to producer middleware. */
export type MutableSendOptions<TNative = unknown> = CommonSendOptions & {
  native?: TNative | undefined;
};

/** Portable nack behaviour. */
export interface CommonNackOptions {
  /** Put the message back in the queue. Default true. */
  requeue?: boolean | undefined;
  /** Delay before the message becomes visible again. */
  delay?: DurationInput | undefined;
}

export interface NackOptions<TNative = unknown> extends CommonNackOptions {
  native?: TNative | undefined;
}

/**
 * Backoff policy for portable (queue-kit strategy) retries. A function form
 * gives full control: `({ attempt, error }) => "30s"`.
 */
export type BackoffPolicy =
  | { strategy: "fixed"; delay: DurationInput }
  | { strategy: "linear"; delay: DurationInput; increment?: DurationInput; maxDelay?: DurationInput }
  | {
      strategy: "exponential";
      delay: DurationInput;
      /** Growth factor. Default 2. */
      factor?: number;
      maxDelay?: DurationInput;
    }
  | {
      /** Exponential with equal jitter (random between 0 and computed). */
      strategy: "jitter";
      delay: DurationInput;
      factor?: number;
      maxDelay?: DurationInput;
    }
  | ((context: { attempt: number; error: unknown }) => DurationInput);

/**
 * Portable worker retry policy. Queue Kit deliberately separates
 * application retry (this) from provider retry (native options) and
 * delivery retry (visibility timeout redelivery).
 */
export interface RetryPolicy {
  /** Total attempts including the first delivery. Default 3. */
  attempts?: number | undefined;
  backoff?: BackoffPolicy | undefined;
  /** Return false to stop retrying for this error. Fatal errors always skip. */
  when?: ((error: unknown) => boolean) | undefined;
}

export type ValidationFailurePolicy = "discard" | "retry" | "dead-letter";
export type PoisonMessagePolicy = "discard" | "dead-letter" | "retry";

/**
 * Portable worker options. `native` stays provider-typed — e.g. BullMQ
 * `lockDuration` or SQS `waitTimeSeconds`/`visibilityTimeout`/
 * `maxNumberOfMessages`.
 */
export interface CommonWorkerOptions {
  /** Parallel handler executions. Default 1. */
  concurrency?: number | undefined;
  /** Long-poll wait passed to pull-based providers. Default "1s". */
  pollInterval?: DurationInput | undefined;
  /** Handler execution timeout — NOT the provider visibility timeout. */
  timeout?: DurationInput | undefined;
  /** Portable retry policy (see RetryPolicy). */
  retry?: RetryPolicy | undefined;
  /** Dead-letter configuration (queue-kit strategy). */
  deadLetter?: { queue: string; afterAttempts?: number } | undefined;
  /** Auto-extend visibility leases while handlers run. Default true where supported. */
  autoExtendVisibility?: boolean | undefined;
  /** Visibility lease applied to received messages (lease-based providers). */
  visibilityTimeout?: DurationInput | undefined;
  /** Cancel polling and (best effort) processing loops. */
  signal?: AbortSignal | undefined;
  /** Start consuming immediately. Default true. */
  autoStart?: boolean | undefined;
  /** Consumer-side payload validation. Default true when the job defines a schema. */
  validation?: { consumer?: boolean } | undefined;
  /** What to do with payloads that fail consumer validation. Default: dead-letter when a DLQ is configured, otherwise discard. */
  validationFailure?: ValidationFailurePolicy | undefined;
  /** What to do with messages that cannot even be decoded. Default "dead-letter" (falls back to discard without a DLQ). */
  onPoisonMessage?: PoisonMessagePolicy | undefined;
}

export interface WorkerOptions<TWorkerNative = unknown> extends CommonWorkerOptions {
  native?: TWorkerNative | undefined;
}

/**
 * Scheduling options. `at` maps onto provider delayed delivery; `cron`/`every`
 * require provider-native scheduling support (`capabilities.scheduling`).
 */
export interface ScheduleOptions<TScheduleNative = unknown> extends CommonSendOptions {
  at?: Date | undefined;
  cron?: string | undefined;
  every?: DurationInput | undefined;
  native?: TScheduleNative | undefined;
}

export interface ScheduleResult {
  strategy: "delayed-delivery" | "provider-native" | "unsupported";
  messageId?: string | undefined;
  native?: unknown;
}

/** A single item for `queue.sendBatch`. */
export interface QueueBatchItem<TData = unknown, TSendNative = unknown> {
  job: JobRefLike;
  data: TData;
  options?: SendOptions<TSendNative> | undefined;
}
