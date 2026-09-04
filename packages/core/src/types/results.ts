import type { QueueCapabilities } from "../capabilities";
import type { ProviderTypes, QueueRef } from "./provider";

/**
 * Result of a successful send. `native` is the provider's own acknowledgement
 * payload, typed through `TTypes["nativeResult"]`.
 */
export interface SendResult<TMessageId = string, TNativeResult = unknown> {
  /** Provider message id. */
  id: TMessageId;
  /** Queue Kit envelope id — stable across retries. */
  envelopeId: string;
  queue: string;
  provider: string;
  timestamp: number;
  deduplication: { mode: "provider" | "adapter" | "none" };
  native: TNativeResult;
}

/**
 * Batch send outcome. Partial failures are explicit and inspectable — never
 * hidden behind an all-or-nothing boolean.
 */
export interface BatchSendResult<TMessageId = string, TNativeResult = unknown> {
  successful: Array<SendResult<TMessageId, TNativeResult>>;
  failed: ReadonlyArray<{
    /** Index of the failed item in the original request. */
    index: number;
    error: unknown;
  }>;
}

export interface QueueSize {
  total: number;
  pending?: number | undefined;
  inflight?: number | undefined;
  delayed?: number | undefined;
}

export interface HealthReport {
  status: "healthy" | "degraded" | "unhealthy" | "unknown";
  latencyMs?: number | undefined;
  provider?: string | undefined;
  detail?: string | undefined;
}

export interface QueueDescription {
  library: "queue-kit";
  version: string;
  provider: string;
  queue: string;
  physicalName: string;
  capabilities: QueueCapabilities;
  retryStrategy: QueueCapabilities["retries"]["details"] extends { strategy: infer S } ? S : unknown;
  deadLetterStrategy: QueueCapabilities["deadLetterQueue"]["details"] extends { strategy: infer S } ? S : unknown;
}

/**
 * A message as delivered by an adapter. `body` is the serialized envelope;
 * `payload` is the already-decoded payload for direct consumers;
 * `native` is the provider handle (e.g. SQS receipt handle wrapper, BullMQ
 * Job) that ack/nack operate on.
 */
export interface ReceivedMessage<T = unknown, TTypes extends ProviderTypes = ProviderTypes> {
  /** Queue Kit envelope id. */
  id: string;
  name: string;
  /** Serialized envelope — the wire truth. */
  body: string;
  payload: T;
  /** Delivery attempt (1-based), tracked via the envelope/adapter. */
  attempt: number;
  /** Epoch ms of the original publish. */
  timestamp: number;
  metadata: Record<string, string>;
  /** Provider-native delivery handle. */
  native: TTypes["nativeMessage"];
  /** Physical queue the message came from. */
  queue: QueueRef;
}

/** Options passed to `adapter.send` — portable fields pre-resolved by core. */
export interface AdapterSendOptions<TNative = unknown> {
  delayMs?: number | undefined;
  idempotencyKey?: string | undefined;
  correlationId?: string | undefined;
  signal?: AbortSignal | undefined;
  native?: TNative | undefined;
}

/** Options passed to `adapter.receive` — portable fields pre-resolved by core. */
export interface AdapterReceiveOptions<TNative = unknown> {
  maxMessages?: number | undefined;
  /** Long-poll wait in ms. */
  waitTimeMs?: number | undefined;
  /** Visibility lease in ms for lease-based providers. */
  visibilityTimeoutMs?: number | undefined;
  signal?: AbortSignal | undefined;
  native?: TNative | undefined;
}

/** Options passed to `adapter.nack`. */
export interface AdapterNackOptions<TNative = unknown> {
  requeue?: boolean | undefined;
  delayMs?: number | undefined;
  native?: TNative | undefined;
}
