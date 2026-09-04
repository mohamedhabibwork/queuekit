import { resolveBackoff } from "./backoff";
import { systemClock, type Clock } from "./clock";
import {
  type CapabilityName,
  type DeadLetterStrategy,
  type QueueCapabilities,
  type RetryStrategy,
} from "./capabilities";
import { parseDuration, type DurationInput } from "./duration";
import { ENVELOPE_VERSION, type OutboundMessage, type QueueEnvelope } from "./envelope";
import { Emitter, type Unsubscribe } from "./events";
import {
  QueueConfigurationError,
  QueueConnectionError,
  QueueKitError,
  QueueMessageTooLargeError,
  QueueSendError,
  QueueSerializationError,
  UnsupportedCapabilityError,
} from "./errors";
import { createId } from "./id";
import { asJobDefinition, jobName, jobVersion, type JobDefinition, type JobRefLike } from "./job";
import type { QueueLogger } from "./logger";
import { compose, type ProducerMiddleware, type SendContext, type WorkerMiddleware } from "./middleware";
import type {
  QueueBatchItem,
  ScheduleOptions,
  ScheduleResult,
  SendOptions,
  WorkerOptions,
} from "./types/options";
import type { AdapterSendOptions, BatchSendResult, HealthReport, QueueDescription, QueueSize, SendResult } from "./types/results";
import type { ProviderTypes, QueueProvider, QueueRef } from "./types/provider";
import type { JobHandler, QueueJob, QueueWorker } from "./types/worker";
import { validateWithSchema, type QueueSchema } from "./schema";
import { JsonSerializer, type QueueSerializer } from "./serializer";
import { QUEUE_KIT_VERSION } from "./version";
import { WorkerEngine, type ResolvedWorkerOptions } from "./worker-engine";

export type PayloadLimitMode = "error" | "warn" | "ignore";

export interface QueueValidationConfig {
  producer?: boolean | undefined;
  consumer?: boolean | undefined;
}

export interface CreateQueueOptions<TTypes extends ProviderTypes> {
  /** Logical queue name — application-owned and portable. */
  name: string;
  provider: QueueProvider<TTypes>;
  serializer?: QueueSerializer | undefined;
  clock?: Clock | undefined;
  logger?: QueueLogger | undefined;
  /** Global producer middleware, applied to every send and batch item. */
  middleware?: readonly ProducerMiddleware<TTypes>[] | undefined;
  /** Encoded-payload size guard against the provider limit. Default "error". */
  payloadLimits?: PayloadLimitMode | undefined;
  validation?: QueueValidationConfig | undefined;
}

/** Events emitted at the queue level. */
export interface QueueEvents<TTypes extends ProviderTypes = ProviderTypes> {
  sent: {
    queue: string;
    jobName: string;
    result: SendResult<TTypes["messageId"], TTypes["nativeResult"]>;
  };
  "send-failed": {
    queue: string;
    jobName: string;
    error: unknown;
  };
  error: {
    queue: string;
    error: unknown;
  };
  closed: {
    queue: string;
  };
}

/**
 * The Queue Kit handle for one logical queue on one provider. All native
 * option types flow from `TTypes`, so `send(..., { native })` autocompletes
 * the *selected provider's* options — the core promise of Queue Kit.
 */
export interface QueueKit<TTypes extends ProviderTypes = ProviderTypes> {
  readonly name: string;
  readonly providerId: string;
  readonly capabilities: QueueCapabilities;
  readonly closed: boolean;

  /** Convenience job definition factory scoped to this queue. */
  job<TData = unknown, TResult = unknown>(name: string): JobDefinition<TData, TResult>;

  send<TData>(
    job: JobDefinition<TData, unknown> | JobRefLike,
    data: TData,
    options?: SendOptions<TTypes["send"]> | undefined,
  ): Promise<SendResult<TTypes["messageId"], TTypes["nativeResult"]>>;

  /** Alias for {@link QueueKit.send} — reads better in job-oriented code. */
  dispatch<TData>(
    job: JobDefinition<TData, unknown> | JobRefLike,
    data: TData,
    options?: SendOptions<TTypes["send"]> | undefined,
  ): Promise<SendResult<TTypes["messageId"], TTypes["nativeResult"]>>;

  sendBatch<TData>(
    items: readonly QueueBatchItem<TData, TTypes["send"]>[],
  ): Promise<BatchSendResult<TTypes["messageId"], TTypes["nativeResult"]>>;

  worker<TData = unknown, TResult = unknown>(
    job: JobDefinition<TData, TResult> | JobRefLike,
    handler: JobHandler<NoInfer<TData>, TResult, TTypes["nativeMessage"]>,
    options?: WorkerOptions<TTypes["worker"]> | undefined,
  ): QueueWorker<TTypes>;

  schedule<TData = unknown>(
    job: JobDefinition<TData, unknown> | JobRefLike,
    data: NoInfer<TData>,
    options: ScheduleOptions<TTypes["schedule"]>,
  ): Promise<ScheduleResult>;

  /** Escape hatch to the provider SDK client — fully typed per provider. */
  native(): TTypes["nativeClient"];
  withNative<TResult>(fn: (native: TTypes["nativeClient"]) => TResult | Promise<TResult>): Promise<TResult>;

  describe(): QueueDescription;
  supports(capability: CapabilityName): boolean;
  assertCapability(capability: CapabilityName): void;

  health(options?: { mode?: "passive" | "active" | undefined } | undefined): Promise<HealthReport>;

  purge(): Promise<void>;
  size(): Promise<QueueSize>;

  /** Register additional global producer middleware. */
  use(...middleware: ProducerMiddleware<TTypes>[]): void;
  on<K extends keyof QueueEvents<TTypes> & string>(
    event: K,
    handler: (payload: QueueEvents<TTypes>[K]) => void | Promise<void>,
  ): Unsubscribe;

  connect(): Promise<void>;
  close(
    options?: { timeout?: DurationInput | undefined; closeWorkers?: boolean | undefined } | undefined,
  ): Promise<void>;
}

export function createQueue<TTypes extends ProviderTypes>(options: CreateQueueOptions<TTypes>): QueueKit<TTypes> {
  return new QueueImpl<TTypes>(options);
}

const ENCODER = new TextEncoder();
type SendOutcome<TTypes extends ProviderTypes> = SendResult<TTypes["messageId"], TTypes["nativeResult"]>;

class QueueImpl<TTypes extends ProviderTypes> implements QueueKit<TTypes> {
  readonly name: string;
  readonly providerId: string;
  readonly capabilities: QueueCapabilities;

  private readonly provider: QueueProvider<TTypes>;
  private readonly ref: QueueRef;
  private readonly serializer: QueueSerializer;
  private readonly clock: Clock;
  private readonly logger: QueueLogger | undefined;
  private readonly validation: QueueValidationConfig;
  private readonly payloadLimits: PayloadLimitMode;
  private readonly events = new Emitter<QueueEvents<TTypes>>();
  private readonly workers = new Set<QueueWorker<TTypes>>();
  private readonly producerMiddleware: ProducerMiddleware<TTypes>[];
  private pipeline: ((context: SendContext<TTypes>, last: () => Promise<SendOutcome<TTypes>>) => Promise<SendOutcome<TTypes>>) | undefined;
  private connectPromise: Promise<void> | undefined;
  private isConnected = false;
  private isClosed = false;

  constructor(options: CreateQueueOptions<TTypes>) {
    if (options.name.length === 0) {
      throw new QueueConfigurationError("Queue name must not be empty");
    }
    this.name = options.name;
    this.provider = options.provider;
    this.providerId = options.provider.id;
    this.capabilities = options.provider.capabilities;
    this.serializer = options.serializer ?? new JsonSerializer();
    this.clock = options.clock ?? systemClock;
    this.logger = options.logger;
    this.validation = options.validation ?? {};
    this.payloadLimits = options.payloadLimits ?? "error";
    this.producerMiddleware = [...(options.middleware ?? [])];
    this.ref = {
      name: options.name,
      physical: options.provider.resolvePhysicalName?.(options.name) ?? options.name,
    };

    if (options.provider.lazy === false) {
      void this.connect().catch((error: unknown) => {
        this.events.emit("error", { queue: this.name, error });
        this.logger?.error?.({ error }, `Failed to connect queue "${this.name}"`);
      });
    }
  }

  get closed(): boolean {
    return this.isClosed;
  }

  job<TData = unknown, TResult = unknown>(name: string): JobDefinition<TData, TResult> {
    return { name, version: undefined, input: undefined, result: undefined };
  }

  // ------------------------------------------------------------------ send

  async send<TData = unknown>(
    job: JobDefinition<TData, unknown> | JobRefLike,
    data: NoInfer<TData>,
    options: SendOptions<TTypes["send"]> = {},
  ): Promise<SendOutcome<TTypes>> {
    this.assertOpen();
    const jobDef = asJobDefinition(job);
    const name = jobName(job);

    await this.validateProducer(name, jobDef?.input as QueueSchema<unknown> | undefined, data);

    const context: SendContext<TTypes> = {
      queue: this.ref,
      provider: this.providerId,
      capabilities: this.capabilities,
      envelope: this.buildEnvelope(name, jobVersion(job), data, options),
      options: { ...options },
    };

    const pipeline = this.getPipeline();
    return pipeline(context, () => this.executeSend(context));
  }

  dispatch<TData = unknown>(
    job: JobDefinition<TData, unknown> | JobRefLike,
    data: NoInfer<TData>,
    options?: SendOptions<TTypes["send"]> | undefined,
  ): Promise<SendOutcome<TTypes>> {
    return this.send(job, data, options);
  }

  private async executeSend(context: SendContext<TTypes>): Promise<SendOutcome<TTypes>> {
    this.assertCapability("send");
    const sendOptions = context.options;

    const delayMs = sendOptions.delay === undefined ? undefined : parseDuration(sendOptions.delay);
    if (delayMs !== undefined && delayMs < 0) {
      throw new QueueConfigurationError("delay must not be negative");
    }
    if (delayMs !== undefined && delayMs > 0) {
      const delayed = this.capabilities.delayedDelivery;
      if (!delayed.supported) {
        throw new UnsupportedCapabilityError("delayedDelivery", undefined, { provider: this.providerId });
      }
      const maxDelayMs = delayed.details?.maxDelayMs;
      if (maxDelayMs !== undefined && delayMs > maxDelayMs) {
        throw new QueueConfigurationError(`delay ${delayMs}ms exceeds the provider maximum of ${maxDelayMs}ms`);
      }
    }

    const message = this.encodeMessage(context.envelope);
    this.checkPayloadSize(message.body, context.envelope.name);

    try {
      const result = await this.provider.adapter.send(this.ref, message, {
        ...(delayMs !== undefined && delayMs > 0 ? { delayMs } : {}),
        ...(sendOptions.idempotencyKey !== undefined ? { idempotencyKey: sendOptions.idempotencyKey } : {}),
        ...(sendOptions.correlationId !== undefined ? { correlationId: sendOptions.correlationId } : {}),
        ...(sendOptions.signal !== undefined ? { signal: sendOptions.signal } : {}),
        ...(sendOptions.native !== undefined ? { native: sendOptions.native } : {}),
      });
      this.events.emit("sent", { queue: this.name, jobName: context.envelope.name, result });
      return result;
    } catch (error) {
      const wrapped =
        error instanceof QueueKitError
          ? error
          : new QueueSendError(`Failed to send "${context.envelope.name}" to "${this.name}"`, {
              cause: error,
              provider: this.providerId,
            });
      this.events.emit("send-failed", { queue: this.name, jobName: context.envelope.name, error: wrapped });
      throw wrapped;
    }
  }

  // ----------------------------------------------------------------- batch

  async sendBatch<TData>(
    items: readonly QueueBatchItem<TData, TTypes["send"]>[],
  ): Promise<BatchSendResult<TTypes["messageId"], TTypes["nativeResult"]>> {
    this.assertOpen();
    this.assertCapability("send");
    if (items.length === 0) {
      return { successful: [], failed: [] };
    }

    const nativeBatch = this.provider.adapter.sendBatch;
    if (nativeBatch !== undefined && this.capabilities.batchSend.supported) {
      const maxItems = this.capabilities.batchSend.details?.maxItems;
      if (maxItems !== undefined && items.length > maxItems) {
        return this.sendBatchChunked(items, maxItems);
      }
      return this.sendBatchNative(items, nativeBatch.bind(this.provider.adapter));
    }
    return this.sendBatchSequential(items);
  }

  private async sendBatchNative<TData>(
    items: readonly QueueBatchItem<TData, TTypes["send"]>[],
    nativeBatch: NonNullable<QueueProvider<TTypes>["adapter"]["sendBatch"]>,
  ): Promise<BatchSendResult<TTypes["messageId"], TTypes["nativeResult"]>> {
    const successful: SendOutcome<TTypes>[] = [];
    const failed: Array<{ index: number; error: unknown }> = [];
    const prepared: Array<{ originalIndex: number; message: OutboundMessage; options: AdapterSendOptions<TTypes["send"]> }> = [];

    for (const [index, item] of items.entries()) {
      try {
        prepared.push({ originalIndex: index, ...(await this.prepareBatchItem(item)) });
      } catch (error) {
        failed.push({ index, error });
      }
    }

    if (prepared.length > 0) {
      const results = await nativeBatch(
        this.ref,
        prepared.map((entry) => ({ message: entry.message, options: entry.options })),
      );
      successful.push(...results.successful);
      for (const failure of results.failed) {
        const original = prepared[failure.index]?.originalIndex ?? failure.index;
        failed.push({ index: original, error: failure.error });
      }
    }
    failed.sort((a, b) => a.index - b.index);
    return { successful, failed };
  }

  private async sendBatchChunked<TData>(
    items: readonly QueueBatchItem<TData, TTypes["send"]>[],
    chunkSize: number,
  ): Promise<BatchSendResult<TTypes["messageId"], TTypes["nativeResult"]>> {
    const successful: SendOutcome<TTypes>[] = [];
    const failed: Array<{ index: number; error: unknown }> = [];
    for (let start = 0; start < items.length; start += chunkSize) {
      const chunk = items.slice(start, start + chunkSize);
      const result = await this.sendBatch(chunk);
      successful.push(...result.successful);
      for (const failure of result.failed) {
        failed.push({ index: failure.index + start, error: failure.error });
      }
    }
    return { successful, failed };
  }

  private async sendBatchSequential<TData>(
    items: readonly QueueBatchItem<TData, TTypes["send"]>[],
  ): Promise<BatchSendResult<TTypes["messageId"], TTypes["nativeResult"]>> {
    const successful: SendOutcome<TTypes>[] = [];
    const failed: Array<{ index: number; error: unknown }> = [];
    for (const [index, item] of items.entries()) {
      try {
        successful.push(await this.send(item.job, item.data, item.options));
      } catch (error) {
        failed.push({ index, error });
      }
    }
    return { successful, failed };
  }

  private async prepareBatchItem<TData>(
    item: QueueBatchItem<TData, TTypes["send"]>,
  ): Promise<{ message: OutboundMessage; options: AdapterSendOptions<TTypes["send"]> }> {
    const name = jobName(item.job);
    const jobDef = asJobDefinition(item.job);
    await this.validateProducer(name, jobDef?.input as QueueSchema<unknown> | undefined, item.data);

    const options = item.options ?? {};
    const delayMs = options.delay === undefined ? undefined : parseDuration(options.delay);
    if (delayMs !== undefined && delayMs > 0 && !this.capabilities.delayedDelivery.supported) {
      throw new UnsupportedCapabilityError("delayedDelivery", undefined, { provider: this.providerId });
    }

    const message = this.encodeMessage(this.buildEnvelope(name, jobVersion(item.job), item.data, options));
    this.checkPayloadSize(message.body, name);

    return {
      message,
      options: {
        ...(delayMs !== undefined && delayMs > 0 ? { delayMs } : {}),
        ...(options.idempotencyKey !== undefined ? { idempotencyKey: options.idempotencyKey } : {}),
        ...(options.correlationId !== undefined ? { correlationId: options.correlationId } : {}),
        ...(options.signal !== undefined ? { signal: options.signal } : {}),
        ...(options.native !== undefined ? { native: options.native } : {}),
      },
    };
  }

  // ---------------------------------------------------------------- worker

  worker<TData = unknown, TResult = unknown>(
    job: JobDefinition<TData, TResult> | JobRefLike,
    handler: JobHandler<NoInfer<TData>, TResult, TTypes["nativeMessage"]>,
    options: WorkerOptions<TTypes["worker"]> = {},
  ): QueueWorker<TTypes> {
    this.assertOpen();
    this.assertCapability("receive");

    const jobDef = asJobDefinition(job);
    const workerId = createId();
    const engine = new WorkerEngine<TTypes>({
      workerId,
      queueName: this.name,
      resolveRef: (name) => ({ name, physical: this.provider.resolvePhysicalName?.(name) ?? name }),
      adapter: this.provider.adapter,
      capabilities: this.capabilities,
      serializer: this.serializer,
      clock: this.clock,
      logger: this.logger,
      provider: this.providerId,
      middleware: [] as WorkerMiddleware[],
      handler: handler as unknown as (job: QueueJob<unknown, TTypes["nativeMessage"]>) => unknown,
      validate: this.buildConsumerValidator(jobDef, options),
      options: this.resolveWorkerOptions(options),
    });

    const handle: QueueWorker<TTypes> = {
      id: workerId,
      queue: this.name,
      get running() {
        return engine.running;
      },
      get paused() {
        return engine.paused;
      },
      start: () => {
        engine.start();
      },
      pause: () => {
        engine.pause();
        return Promise.resolve();
      },
      resume: () => {
        engine.resume();
        return Promise.resolve();
      },
      close: async (closeOptions?: { timeout?: DurationInput; force?: boolean }) => {
        await engine.close(closeOptions);
        this.workers.delete(handle);
      },
      on: (event, eventHandler) => engine.events.on(event, eventHandler as never),
    };

    this.workers.add(handle);
    if (options.autoStart ?? true) {
      engine.start();
    }
    return handle;
  }

  private resolveWorkerOptions(options: WorkerOptions<TTypes["worker"]>): ResolvedWorkerOptions {
    const concurrency = options.concurrency ?? 1;
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new QueueConfigurationError(`concurrency must be a positive integer, received ${String(concurrency)}`);
    }
    const retry = options.retry;
    return {
      concurrency,
      pollIntervalMs: parseDuration(options.pollInterval ?? "1s"),
      timeoutMs: options.timeout === undefined ? undefined : parseDuration(options.timeout),
      retryAttempts: retry?.attempts ?? 3,
      backoff: resolveBackoff(retry?.backoff),
      retryWhen: retry?.when,
      deadLetterQueue: options.deadLetter?.queue,
      deadLetterAfterAttempts: options.deadLetter?.afterAttempts,
      autoExtendVisibility: options.autoExtendVisibility ?? true,
      visibilityTimeoutMs: options.visibilityTimeout === undefined ? undefined : parseDuration(options.visibilityTimeout),
      signal: options.signal,
      validationFailure: options.validationFailure,
      onPoisonMessage: options.onPoisonMessage,
      native: options.native,
    };
  }

  private buildConsumerValidator<TData, TResult>(
    jobDef: JobDefinition<TData, TResult> | undefined,
    options: WorkerOptions<TTypes["worker"]>,
  ): ((envelope: QueueEnvelope) => Promise<void>) | undefined {
    if (jobDef?.input === undefined) return undefined;
    const enabled = options.validation?.consumer ?? this.validation.consumer ?? true;
    if (!enabled) return undefined;
    const schema = jobDef.input as QueueSchema<unknown>;
    return async (envelope) => {
      await validateWithSchema(schema, envelope.payload, jobDef.name);
    };
  }

  // -------------------------------------------------------------- schedule

  async schedule<TData = unknown>(
    job: JobDefinition<TData, unknown> | JobRefLike,
    data: NoInfer<TData>,
    options: ScheduleOptions<TTypes["schedule"]>,
  ): Promise<ScheduleResult> {
    if (options.cron !== undefined || options.every !== undefined) {
      throw new UnsupportedCapabilityError(
        "scheduling",
        this.capabilities.scheduling.supported
          ? `Provider "${this.providerId}" schedules recurring work through its native options — pass them via options.native or use queue.native().`
          : `Provider "${this.providerId}" has no native scheduler. Use a scheduler service or application cron that calls queue.send().`,
        { provider: this.providerId },
      );
    }

    if (options.at !== undefined) {
      const delayMs = Math.max(0, options.at.getTime() - this.clock.now());
      const result = await this.send(job, data, { ...options, delay: delayMs });
      return { strategy: "delayed-delivery", messageId: String(result.id), native: result.native };
    }

    throw new QueueConfigurationError("schedule() requires one of: at, cron, every");
  }

  // ---------------------------------------------------------------- native

  native(): TTypes["nativeClient"] {
    const getNative = this.provider.adapter.nativeClient;
    if (getNative === undefined) {
      throw new UnsupportedCapabilityError(
        "nativeClient",
        `Provider "${this.providerId}" does not expose a native client`,
        { provider: this.providerId },
      );
    }
    return getNative.call(this.provider.adapter);
  }

  async withNative<TResult>(fn: (native: TTypes["nativeClient"]) => TResult | Promise<TResult>): Promise<TResult> {
    await this.connect();
    return fn(this.native());
  }

  // ------------------------------------------------------------- operations

  describe(): QueueDescription {
    const retryStrategy: RetryStrategy =
      (this.capabilities.retries.details as { strategy?: RetryStrategy } | undefined)?.strategy ?? "none";
    const deadLetterStrategy: DeadLetterStrategy =
      (this.capabilities.deadLetterQueue.details as { strategy?: DeadLetterStrategy } | undefined)?.strategy ?? "none";
    return {
      library: "queue-kit",
      version: QUEUE_KIT_VERSION,
      provider: this.providerId,
      queue: this.name,
      physicalName: this.ref.physical,
      capabilities: this.capabilities,
      retryStrategy,
      deadLetterStrategy,
    };
  }

  supports(capability: CapabilityName): boolean {
    return this.capabilities[capability].supported;
  }

  assertCapability(capability: CapabilityName): void {
    if (!this.capabilities[capability].supported) {
      throw new UnsupportedCapabilityError(capability, undefined, { provider: this.providerId });
    }
  }

  async health(options: { mode?: "passive" | "active" | undefined } = {}): Promise<HealthReport> {
    const startedAt = Date.now();
    const mode = options.mode ?? "passive";
    try {
      const providerHealth = this.provider.adapter.health;
      if (providerHealth !== undefined) {
        const report = await providerHealth.call(this.provider.adapter);
        return { provider: this.providerId, ...report, latencyMs: report.latencyMs ?? Date.now() - startedAt };
      }
      if (mode === "active" && this.provider.adapter.size !== undefined) {
        await this.provider.adapter.size(this.ref);
        return { status: "healthy", latencyMs: Date.now() - startedAt, provider: this.providerId };
      }
      return { status: "unknown", provider: this.providerId, detail: "Provider does not implement a health check" };
    } catch (error) {
      return {
        status: "unhealthy",
        latencyMs: Date.now() - startedAt,
        provider: this.providerId,
        detail: error instanceof Error ? error.message : String(error),
      };
    }
  }

  async purge(): Promise<void> {
    const purge = this.provider.adapter.purge;
    if (purge === undefined) {
      throw new UnsupportedCapabilityError("purge", `Provider "${this.providerId}" does not support purge`, {
        provider: this.providerId,
      });
    }
    await purge.call(this.provider.adapter, this.ref);
  }

  async size(): Promise<QueueSize> {
    const size = this.provider.adapter.size;
    if (size === undefined) {
      throw new UnsupportedCapabilityError("size", `Provider "${this.providerId}" does not support size`, {
        provider: this.providerId,
      });
    }
    const result = await size.call(this.provider.adapter, this.ref);
    return typeof result === "number" ? { total: result } : result;
  }

  use(...middleware: ProducerMiddleware<TTypes>[]): void {
    this.producerMiddleware.push(...middleware);
    this.pipeline = undefined;
  }

  on<K extends keyof QueueEvents<TTypes> & string>(
    event: K,
    handler: (payload: QueueEvents<TTypes>[K]) => void | Promise<void>,
  ): Unsubscribe {
    return this.events.on(event, handler);
  }

  async connect(): Promise<void> {
    if (this.isClosed) {
      throw new QueueConnectionError(`Queue "${this.name}" is closed`, { provider: this.providerId });
    }
    if (this.isConnected) return;
    if (this.connectPromise !== undefined) return this.connectPromise;

    this.connectPromise = (async () => {
      await this.provider.adapter.connect?.();
      this.isConnected = true;
    })().catch((error: unknown) => {
      this.connectPromise = undefined;
      throw error instanceof QueueKitError
        ? error
        : new QueueConnectionError(`Failed to connect queue "${this.name}"`, {
            cause: error,
            provider: this.providerId,
          });
    });
    return this.connectPromise;
  }

  async close(
    options: { timeout?: DurationInput | undefined; closeWorkers?: boolean | undefined } = {},
  ): Promise<void> {
    if (this.isClosed) return;
    this.isClosed = true;

    if (options.closeWorkers ?? true) {
      const closeOptions = options.timeout === undefined ? {} : { timeout: options.timeout };
      await Promise.allSettled([...this.workers].map((worker) => worker.close(closeOptions)));
      this.workers.clear();
    }

    try {
      await this.provider.adapter.disconnect?.();
    } finally {
      await this.provider.adapter.close();
      this.isConnected = false;
      this.connectPromise = undefined;
    }

    this.events.emit("closed", { queue: this.name });
    this.events.removeAllListeners();
  }

  // --------------------------------------------------------------- helpers

  private assertOpen(): void {
    if (this.isClosed) {
      throw new QueueConnectionError(`Queue "${this.name}" is closed`, { provider: this.providerId });
    }
  }

  private getPipeline(): NonNullable<QueueImpl<TTypes>["pipeline"]> {
    if (this.pipeline === undefined) {
      this.pipeline = compose<SendContext<TTypes>, SendOutcome<TTypes>>(this.producerMiddleware);
    }
    return this.pipeline;
  }

  private async validateProducer(
    name: string,
    schema: QueueSchema<unknown> | undefined,
    data: unknown,
  ): Promise<void> {
    if (schema === undefined) return;
    if ((this.validation.producer ?? true) !== true) return;
    try {
      await validateWithSchema(schema, data, name);
    } catch (error) {
      this.events.emit("error", { queue: this.name, error });
      throw error;
    }
  }

  private buildEnvelope<TData>(
    name: string,
    version: number | undefined,
    payload: TData,
    options: SendOptions<TTypes["send"]>,
  ): QueueEnvelope<TData> {
    const metadata: Record<string, string> = { ...(options.metadata ?? {}) };
    if (options.correlationId !== undefined) metadata["correlation-id"] = options.correlationId;
    if (options.idempotencyKey !== undefined) metadata["idempotency-key"] = options.idempotencyKey;

    return {
      v: ENVELOPE_VERSION,
      id: createId(),
      name,
      ...(version !== undefined ? { version } : {}),
      payload,
      timestamp: this.clock.now(),
      attempt: 1,
      ...(Object.keys(metadata).length > 0 ? { metadata } : {}),
      ...(options.trace !== undefined ? { trace: options.trace } : {}),
    };
  }

  private encodeMessage<T>(envelope: QueueEnvelope<T>): OutboundMessage<T> {
    let body: string | Uint8Array;
    try {
      body = this.serializer.encode(envelope);
    } catch (error) {
      throw new QueueSerializationError(`Failed to serialize payload for "${envelope.name}"`, { cause: error });
    }
    return { envelope, body };
  }

  private checkPayloadSize(body: string | Uint8Array, name: string): void {
    if (this.payloadLimits === "ignore") return;
    const maxBytes = this.capabilities.send.details?.maxBytes;
    if (maxBytes === undefined) return;
    const sizeBytes = typeof body === "string" ? ENCODER.encode(body).length : body.length;
    if (sizeBytes <= maxBytes) return;
    const error = new QueueMessageTooLargeError(
      `Encoded payload for "${name}" is ${sizeBytes} bytes, exceeding the provider limit of ${maxBytes} bytes on "${this.name}"`,
      { sizeBytes, maxBytes, provider: this.providerId },
    );
    if (this.payloadLimits === "error") throw error;
    this.logger?.warn?.({ sizeBytes, maxBytes, queue: this.name, job: name }, "payload exceeds provider limit");
  }
}
