// Primitives
export { parseDuration, type DurationInput } from "./duration";
export { FakeClock, systemClock, guardUnref, type Clock, type FakeTimeout } from "./clock";
export { createId } from "./id";
export { JsonSerializer, type QueueSerializer } from "./serializer";
export {
  ENVELOPE_VERSION,
  isEnvelopeLike,
  type EnvelopeLike,
  type OutboundMessage,
  type QueueEnvelope,
  type TraceContext,
} from "./envelope";
export { validateWithSchema, type QueueSchema, type StandardIssue, type ValidationOutcome } from "./schema";
export { createConsoleLogger, type LogLevel, type QueueLogger } from "./logger";
export {
  CAPABILITY_NAMES,
  createCapabilities,
  type BatchCapabilityDetails,
  type Capability,
  type CapabilityName,
  type DelayCapabilityDetails,
  type DeadLetterStrategy,
  type FifoCapabilityDetails,
  type QueueCapabilities,
  type RetryCapabilityDetails,
  type RetryStrategy,
  type SendCapabilityDetails,
} from "./capabilities";

// Errors
export {
  fatal,
  isFatalError,
  retryable,
  QueueAckError,
  QueueAuthenticationError,
  QueueConfigurationError,
  QueueConnectionError,
  QueueFatalError,
  QueueKitError,
  QueueMessageTooLargeError,
  QueueRateLimitError,
  QueueReceiveError,
  QueueRetryableError,
  QueueSendError,
  QueueSerializationError,
  QueueTimeoutError,
  QueueValidationError,
  QueueWorkerError,
  UnsupportedCapabilityError,
  type QueueKitErrorOptions,
} from "./errors";

// Provider contract
export {
  defineQueueProvider,
  type AnyProviderTypes,
  type ProviderConfig,
  type ProviderTypes,
  type QueueAdapter,
  type QueueAdapterFactory,
  type QueueProvider,
  type QueueRef,
} from "./types/provider";

// Options
export {
  type BackoffPolicy,
  type CommonNackOptions,
  type CommonSendOptions,
  type CommonWorkerOptions,
  type MutableSendOptions,
  type NackOptions,
  type PoisonMessagePolicy,
  type QueueBatchItem,
  type RetryPolicy,
  type ScheduleOptions,
  type ScheduleResult,
  type SendOptions,
  type ValidationFailurePolicy,
  type WorkerOptions,
} from "./types/options";

// Results & messages
export {
  type AdapterNackOptions,
  type AdapterReceiveOptions,
  type AdapterSendOptions,
  type BatchSendResult,
  type HealthReport,
  type QueueDescription,
  type QueueSize,
  type ReceivedMessage,
  type SendResult,
} from "./types/results";

// Workers & jobs
export {
  defineJob,
  type DefineJobOptions,
  type JobDefinition,
  type JobRef,
} from "./job";
export {
  type JobHandler,
  type JobSettlement,
  type QueueJob,
  type QueueWorker,
  type WorkerEvents,
} from "./types/worker";

// Middleware & events
export {
  compose,
  type ProducerMiddleware,
  type SendContext,
  type WorkerContext,
  type WorkerMiddleware,
} from "./middleware";
export { Emitter, type EventHandler, type Unsubscribe } from "./events";

// Engine & backoff (exported for advanced adapters embedding the engine)
export { computeBackoff, resolveBackoff, type ResolvedBackoff } from "./backoff";
export { WorkerEngine, type ResolvedWorkerOptions, type WorkerEngineConfig, type WorkerEngineState } from "./worker-engine";

// Public API
export {
  createQueue,
  type CreateQueueOptions,
  type PayloadLimitMode,
  type QueueEvents,
  type QueueKit,
  type QueueValidationConfig,
} from "./queue";
export { createQueueRegistry, type QueueRegistry } from "./registry";
export { installGracefulShutdown, type GracefulShutdownOptions } from "./graceful-shutdown";
export { QUEUE_KIT_VERSION } from "./version";
