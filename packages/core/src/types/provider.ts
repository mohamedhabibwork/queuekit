import type { Capability, QueueCapabilities } from "../capabilities";
import type { QueueEnvelope, OutboundMessage } from "../envelope";
import type { QueueSchema } from "../schema";
import type { DurationInput } from "../duration";
import type {
  AdapterSendOptions,
  AdapterReceiveOptions,
  AdapterNackOptions,
  BatchSendResult,
  HealthReport,
  QueueSize,
  ReceivedMessage,
  SendResult,
} from "./results";

/**
 * The type registry every provider fills in. All native surfaces flow
 * through these members, which is what keeps native options strongly typed:
 * pick `sqs(...)` and `TTypes["send"]` becomes SQS send options — pick
 * `bullmq(...)` and it becomes BullMQ job options. No flattening, no
 * `Record<string, unknown>`.
 */
export interface ProviderTypes {
  /** Options used to connect to the provider (constructor-level). */
  connection: unknown;
  /** Reserved: per-queue creation native options. */
  queue: unknown;
  /** Native send options (BullMQ `JobsOptions`, SQS `SendMessage` extras, ...). */
  send: unknown;
  /** Native receive options for pull-based consumption. */
  receive: unknown;
  /** Native worker options (locking, polling, prefetch, ...). */
  worker: unknown;
  /** Native negative-acknowledgement options. */
  nack: unknown;
  /** Native scheduling/repeat options for recurring jobs. */
  schedule: unknown;
  /** Provider message id type. */
  messageId: string;
  /** The native delivery handle used for ack/nack and exposed on jobs. */
  nativeMessage: unknown;
  /** The provider's own client/queue object exposed via `queue.native()`. */
  nativeClient: unknown;
  /** The provider's send acknowledgement payload, typed on `SendResult.native`. */
  nativeResult: unknown;
}

/** Any valid provider type registry. */
export type AnyProviderTypes = ProviderTypes;

/**
 * Logical ↔ physical queue address. The logical name is application-owned
 * ("emails"); the physical name is provider-owned (an SQS URL, a Redis key
 * prefix, an AMQP queue name).
 */
export interface QueueRef {
  readonly name: string;
  readonly physical: string;
}

/**
 * The contract a provider adapter implements. Queue Kit core drives this
 * interface exclusively — adapters never see Queue Kit's Queue class.
 *
 * Optional members are guarded by `QueueCapabilities`: before calling any
 * optional operation core asserts the matching capability so unsupported
 * operations fail loudly instead of silently doing nothing.
 */
export interface QueueAdapter<TTypes extends ProviderTypes> {
  /** Short provider identifier, e.g. "memory", "aws-sqs", "bullmq". */
  readonly id: string;
  readonly capabilities: QueueCapabilities;

  connect?(): Promise<void>;
  disconnect?(): Promise<void>;

  send<T>(
    queue: QueueRef,
    message: OutboundMessage<T>,
    options: AdapterSendOptions<TTypes["send"]>,
  ): Promise<SendResult<TTypes["messageId"], TTypes["nativeResult"]>>;

  sendBatch?<T>(
    queue: QueueRef,
    messages: readonly { readonly message: OutboundMessage<T>; readonly options: AdapterSendOptions<TTypes["send"]> }[],
  ): Promise<BatchSendResult<TTypes["messageId"], TTypes["nativeResult"]>>;

  receive?<T>(
    queue: QueueRef,
    options: AdapterReceiveOptions<TTypes["receive"]>,
  ): Promise<readonly ReceivedMessage<T, TTypes>[]>;

  ack?(message: ReceivedMessage<unknown, TTypes>): Promise<void>;

  nack?(
    message: ReceivedMessage<unknown, TTypes>,
    options?: AdapterNackOptions<TTypes["nack"]>,
  ): Promise<void>;

  /** Extend the visibility lease of an in-flight message (lease-based providers). */
  extendVisibility?(
    message: ReceivedMessage<unknown, TTypes>,
    extraMs: number,
  ): Promise<void>;

  purge?(queue: QueueRef): Promise<void>;

  size?(queue: QueueRef): Promise<number | QueueSize>;

  health?(): Promise<HealthReport>;

  /** Escape hatch: the provider SDK client or queue object, fully typed. */
  nativeClient?(): TTypes["nativeClient"];

  /** Release sockets, clients, timers and polling loops. */
  close(): Promise<void>;

  /** Redact credentials for logging — never leak secrets. */
  redactConfig?(): Record<string, unknown>;
}

/** Configuration contract every provider factory accepts. */
export interface ProviderConfig<TTypes extends ProviderTypes> {
  connection: TTypes["connection"];
  /** When false, Queue Kit connects eagerly. Default: lazy. */
  lazy?: boolean;
  /** Map a logical queue name to the provider's physical name. Default: identity. */
  resolvePhysicalName?: (logicalName: string) => string;
}

export interface QueueProvider<TTypes extends ProviderTypes> {
  readonly id: string;
  readonly capabilities: QueueCapabilities;
  readonly adapter: QueueAdapter<TTypes>;
  readonly connection: TTypes["connection"];
  readonly lazy: boolean;
  resolvePhysicalName?(logicalName: string): string;
}

export type QueueAdapterFactory<TTypes extends ProviderTypes> = (
  config: ProviderConfig<TTypes>,
) => QueueProvider<TTypes>;

/**
 * Identity helper that gives third-party adapters first-class typing without
 * touching core:
 *
 * ```ts
 * export const nats = defineQueueProvider((config) => ({ ... }));
 * ```
 */
export function defineQueueProvider<TTypes extends ProviderTypes>(
  factory: QueueAdapterFactory<TTypes>,
): QueueAdapterFactory<TTypes> {
  return factory;
}

// Re-exported for adapter author convenience.
export type { Capability, QueueEnvelope, QueueSchema, DurationInput };
