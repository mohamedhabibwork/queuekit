import { computeBackoff, type ResolvedBackoff } from "./backoff";
import type { Clock } from "./clock";
import { ENVELOPE_VERSION, isEnvelopeLike, type QueueEnvelope } from "./envelope";
import {
  isFatalError,
  QueueSerializationError,
  QueueTimeoutError,
  QueueWorkerError,
  UnsupportedCapabilityError,
} from "./errors";
import { createId } from "./id";
import type { QueueCapabilities } from "./capabilities";
import type { QueueLogger } from "./logger";
import { compose, type WorkerContext, type WorkerMiddleware } from "./middleware";
import { parseDuration, type DurationInput } from "./duration";
import type { QueueSerializer } from "./serializer";
import type { PoisonMessagePolicy, ValidationFailurePolicy } from "./types/options";
import type { ProviderTypes, QueueAdapter, QueueRef } from "./types/provider";
import type { ReceivedMessage } from "./types/results";
import type { JobInternals, JobSettlement, QueueJob, WorkerEvents } from "./types/worker";
import { Emitter } from "./events";
import { guardUnref } from "./clock";

/** Maximum messages requested per receive call. */
const MAX_BATCH = 10;
/** Backoff progression when the adapter's receive keeps failing. */
const RECEIVE_ERROR_BACKOFF_MAX_MS = 10_000;
const POISON_SNIPPET_BYTES = 2_048;

/**
 * Worker options with every portable field pre-resolved (durations parsed,
 * defaults applied). Built by `Queue.worker()` — never constructed by users.
 */
export interface ResolvedWorkerOptions {
  concurrency: number;
  pollIntervalMs: number;
  timeoutMs: number | undefined;
  /** Total attempts including the first delivery. */
  retryAttempts: number;
  backoff: ResolvedBackoff;
  retryWhen: ((error: unknown) => boolean) | undefined;
  deadLetterQueue: string | undefined;
  /** Overrides retryAttempts as the dead-letter threshold when set. */
  deadLetterAfterAttempts: number | undefined;
  autoExtendVisibility: boolean;
  visibilityTimeoutMs: number | undefined;
  signal: AbortSignal | undefined;
  validationFailure: ValidationFailurePolicy | undefined;
  onPoisonMessage: PoisonMessagePolicy | undefined;
  native: unknown;
}

export interface WorkerEngineConfig<TTypes extends ProviderTypes> {
  readonly workerId: string;
  readonly queueName: string;
  readonly resolveRef: (name: string) => QueueRef;
  readonly adapter: QueueAdapter<TTypes>;
  readonly capabilities: QueueCapabilities;
  readonly serializer: QueueSerializer;
  readonly clock: Clock;
  readonly logger: QueueLogger | undefined;
  readonly provider: string;
  readonly middleware: readonly WorkerMiddleware[];
  readonly handler: (job: QueueJob<unknown, TTypes["nativeMessage"]>) => unknown;
  /** Throws QueueValidationError on invalid payloads; undefined when no schema. */
  readonly validate: ((envelope: QueueEnvelope) => Promise<void>) | undefined;
  readonly options: ResolvedWorkerOptions;
}

export type WorkerEngineState = "idle" | "running" | "paused" | "stopping" | "stopped";

/**
 * The portable worker engine. Drives any adapter that implements `receive`
 * through the same lifecycle: poll → decode → validate → middleware →
 * handler → settle (ack / portable retry / dead-letter / discard).
 *
 * Retry strategy is "queue-kit" by default: on a retryable failure the engine
 * re-publishes the envelope with `attempt + 1` and the computed backoff, then
 * acknowledges the original delivery. This works on every provider that has
 * `send`, at the cost of at-least-once duplication on crash windows — the
 * standard trade-off, documented rather than hidden.
 */
export class WorkerEngine<TTypes extends ProviderTypes> {
  readonly events = new Emitter<WorkerEvents<TTypes>>();

  private currentState: WorkerEngineState = "idle";
  private readonly loopAbort = new AbortController();
  private readonly jobAbort = new AbortController();
  private readonly active = new Set<Promise<void>>();
  private readonly resumeWaiters: Array<() => void> = [];
  private readonly composed: (context: WorkerContext, last: () => Promise<unknown>) => Promise<unknown>;
  private loopPromise: Promise<void> | undefined;
  private closePromise: Promise<void> | undefined;

  constructor(private readonly config: WorkerEngineConfig<TTypes>) {
    this.composed = compose(config.middleware);
    if (config.options.signal !== undefined) {
      config.options.signal.addEventListener(
        "abort",
        () => {
          this.stop();
        },
        { once: true },
      );
    }
  }

  get workerId(): string {
    return this.config.workerId;
  }

  get phase(): WorkerEngineState {
    return this.currentState;
  }

  get running(): boolean {
    return this.currentState === "running" || this.currentState === "paused";
  }

  get paused(): boolean {
    return this.currentState === "paused";
  }

  start(): void {
    if (this.currentState !== "idle") return;
    if (this.config.adapter.receive === undefined) {
      throw new UnsupportedCapabilityError(
        "receive",
        `Provider "${this.config.provider}" does not implement receive(), so it cannot host a Queue Kit worker. Use native dispatch/push delivery instead.`,
        { provider: this.config.provider },
      );
    }
    if (this.loopAbort.signal.aborted) {
      throw new QueueWorkerError("Worker engine was already closed");
    }
    this.currentState = "running";
    this.loopPromise = this.runLoop();
    this.events.emit("started", { workerId: this.config.workerId });
  }

  pause(): void {
    if (this.currentState !== "running") return;
    this.currentState = "paused";
    this.events.emit("paused", { workerId: this.config.workerId });
  }

  resume(): void {
    if (this.currentState !== "paused") return;
    this.currentState = "running";
    this.wakeResumeWaiters();
    this.events.emit("resumed", { workerId: this.config.workerId });
  }

  /** Internal: stop the polling loop; active jobs continue. */
  private stop(): void {
    if (this.currentState === "stopped" || this.currentState === "stopping") return;
    this.currentState = "stopping";
    this.loopAbort.abort();
    this.wakeResumeWaiters();
  }

  /**
   * Graceful shutdown: stop fetching new work, let active jobs finish within
   * `timeout`, release resources. `force: true` additionally aborts active
   * handlers through `job.signal`.
   */
  close(options: { timeout?: DurationInput; force?: boolean } = {}): Promise<void> {
    if (this.closePromise !== undefined) return this.closePromise;
    this.closePromise = this.performClose(options);
    return this.closePromise;
  }

  private async performClose(options: { timeout?: DurationInput; force?: boolean }): Promise<void> {
    if (this.currentState === "idle") {
      this.currentState = "stopped";
      return;
    }
    this.stop();
    if (options.force === true) {
      this.jobAbort.abort(new QueueWorkerError("Worker force-closed; active handlers were aborted"));
    }
    // Shutdown timeouts are real wall-clock time on purpose: they must work
    // even when the queue runs on a FakeClock that nobody advances.
    const timeoutMs = options.timeout === undefined ? 30_000 : parseDuration(options.timeout);
    await Promise.race([
      Promise.allSettled([...this.active]),
      realDelay(timeoutMs),
    ]);
    this.currentState = "stopped";
    this.events.emit("stopped", { workerId: this.config.workerId });
    await this.loopPromise?.catch(() => undefined);
  }

  // ------------------------------------------------------------------ loop

  private async runLoop(): Promise<void> {
    const { adapter, clock, options } = this.config;
    const ref = this.config.resolveRef(this.config.queueName);
    const signal = this.loopAbort.signal;
    let consecutiveErrors = 0;

    while (this.currentState === "running" || this.currentState === "paused") {
      if (signal.aborted) break;

      if (this.currentState === "paused") {
        await this.waitForResume(signal);
        continue;
      }

      const freeSlots = options.concurrency - this.active.size;
      if (freeSlots <= 0) {
        await Promise.race([...this.active, this.waitForAbort(signal)]);
        continue;
      }

      let messages: readonly ReceivedMessage<unknown, TTypes>[];
      try {
        messages = await adapter.receive!(ref, {
          maxMessages: Math.min(freeSlots, MAX_BATCH),
          waitTimeMs: options.pollIntervalMs,
          ...(options.visibilityTimeoutMs !== undefined ? { visibilityTimeoutMs: options.visibilityTimeoutMs } : {}),
          signal,
          ...(options.native !== undefined ? { native: options.native } : {}),
        });
        consecutiveErrors = 0;
      } catch (error) {
        if (signal.aborted) break;
        consecutiveErrors += 1;
        this.emitError(error);
        const backoffMs = Math.min(RECEIVE_ERROR_BACKOFF_MAX_MS, 100 * 2 ** (consecutiveErrors - 1));
        await clock.delay(backoffMs, signal).catch(() => undefined);
        continue;
      }

      for (const message of messages) {
        if (this.phase === "paused") {
          // Paused while the receive was in flight: give the messages back
          // instead of processing them (or losing them).
          await this.nackQuietly(message, { requeue: true });
          continue;
        }
        if (this.currentState !== "running") break;
        const task = this.process(message).catch((error: unknown) => this.emitError(error));
        this.active.add(task);
        void task.then(
          () => {
            this.active.delete(task);
          },
          () => {
            this.active.delete(task);
          },
        );
      }

      if (messages.length === 0) {
        // Adapters that long-poll internally rarely land here; those that
        // return immediately use this delay to avoid a hot loop.
        await clock.delay(options.pollIntervalMs, signal).catch(() => undefined);
      }
    }
  }

  private waitForResume(signal: AbortSignal): Promise<void> {
    return new Promise<void>((resolve) => {
      const waiter = (): void => {
        signal.removeEventListener("abort", onAbort);
        resolve();
      };
      const onAbort = (): void => {
        const index = this.resumeWaiters.indexOf(waiter);
        if (index >= 0) this.resumeWaiters.splice(index, 1);
        resolve();
      };
      this.resumeWaiters.push(waiter);
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  private waitForAbort(signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const onAbort = (): void => {
        signal.removeEventListener("abort", onAbort);
        resolve();
      };
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  private wakeResumeWaiters(): void {
    for (const waiter of this.resumeWaiters.splice(0)) {
      waiter();
    }
  }

  // -------------------------------------------------------------- pipeline

  private async process(message: ReceivedMessage<unknown, TTypes>): Promise<void> {
    const { clock } = this.config;
    const startedAt = clock.now();

    const envelope = this.tryDecode(message);
    if (envelope === undefined) {
      await this.handlePoisonMessage(message);
      return;
    }
    envelope.attempt = message.attempt;

    this.events.emit("received", {
      workerId: this.config.workerId,
      messageId: message.id,
      jobName: envelope.name,
      attempt: message.attempt,
      native: message.native,
    });

    const internals: JobInternals = { settled: null, retryDelay: undefined };
    const jobSignalController = new AbortController();
    const forwardAbort = (): void => {
      jobSignalController.abort(this.jobAbort.signal.reason);
    };
    this.jobAbort.signal.addEventListener("abort", forwardAbort, { once: true });

    const extender = this.startVisibilityExtender(message, jobSignalController.signal);

    try {
      if (this.config.validate !== undefined) {
        try {
          await this.config.validate(envelope);
        } catch (error) {
          await this.handleValidationFailure(message, envelope, error, internals);
          return;
        }
      }

      const job = this.createJob(message, envelope, internals, jobSignalController.signal);
      const context: WorkerContext = { job, envelope };
      const execution = Promise.resolve(this.composed(context, async () => this.config.handler(job)));

      const result = await this.withTimeout(execution, envelope.name);

      if (internals.settled === "retry") {
        await this.retryMessage(message, envelope, new QueueWorkerError("job.retry() requested"), internals.retryDelay);
        return;
      }

      await this.ackQuietly(message);
      this.events.emit("completed", {
        workerId: this.config.workerId,
        messageId: message.id,
        jobName: envelope.name,
        attempt: message.attempt,
        durationMs: clock.now() - startedAt,
        result,
      });
    } catch (error) {
      await this.handleFailure(message, envelope, error, internals);
    } finally {
      extender.stop();
      this.jobAbort.signal.removeEventListener("abort", forwardAbort);
    }
  }

  private tryDecode(message: ReceivedMessage<unknown, TTypes>): QueueEnvelope | undefined {
    try {
      const decoded: unknown = this.config.serializer.decode(message.body);
      if (!isEnvelopeLike(decoded)) {
        throw new QueueSerializationError("Decoded message body is not a Queue Kit envelope");
      }
      if (decoded.v !== ENVELOPE_VERSION) {
        throw new QueueSerializationError(
          `Unsupported envelope version ${String(decoded.v)} (expected ${ENVELOPE_VERSION})`,
        );
      }
      return decoded;
    } catch (error) {
      this.emitError(new QueueSerializationError(`Message ${message.id} could not be decoded`, { cause: error }));
      return undefined;
    }
  }

  private withTimeout(execution: Promise<unknown>, jobName: string): Promise<unknown> {
    const timeoutMs = this.config.options.timeoutMs;
    if (timeoutMs === undefined) return execution;

    // Swallow late rejections of the losing branch so neither the handler
    // result nor the timer can surface as an unhandled rejection.
    const guarded = (promise: Promise<unknown>): Promise<unknown> => {
      promise.catch(() => undefined);
      return promise;
    };
    const timeout = this.config.clock.delay(timeoutMs).then(() => {
      throw new QueueTimeoutError(`Handler for "${jobName}" exceeded the processing timeout of ${timeoutMs}ms`);
    });
    return Promise.race([guarded(execution), guarded(timeout)]);
  }

  private createJob(
    message: ReceivedMessage<unknown, TTypes>,
    envelope: QueueEnvelope,
    internals: JobInternals,
    signal: AbortSignal,
  ): QueueJob<unknown, TTypes["nativeMessage"]> {
    const settle = (settlement: JobSettlement, delayMs?: number): void => {
      internals.settled = settlement;
      internals.retryDelay = delayMs;
    };
    return {
      id: envelope.id,
      name: envelope.name,
      version: envelope.version,
      data: envelope.payload,
      attempt: message.attempt,
      maxAttempts: this.config.options.retryAttempts,
      metadata: { ...message.metadata },
      timestamp: envelope.timestamp,
      signal,
      native: message.native,
      ack: async () => {
        settle("ack");
        await this.ackQuietly(message);
      },
      discard: async () => {
        settle("discard");
        await this.ackQuietly(message);
      },
      retry: async (options?: { delay?: DurationInput }) => {
        settle("retry", options?.delay === undefined ? undefined : parseDuration(options.delay));
      },
    };
  }

  // ------------------------------------------------------------ settlement

  private async handleFailure(
    message: ReceivedMessage<unknown, TTypes>,
    envelope: QueueEnvelope,
    error: unknown,
    internals: JobInternals,
  ): Promise<void> {
    const options = this.config.options;
    const attempt = message.attempt;
    const maxAttempts = options.deadLetterAfterAttempts ?? options.retryAttempts;
    const explicitlyRequested = internals.settled === "retry";
    const policyAllows =
      !isFatalError(error) && (options.retryWhen === undefined || safeWhen(options.retryWhen, error));
    const willRetry = attempt < maxAttempts && (explicitlyRequested || policyAllows);

    this.events.emit("failed", {
      workerId: this.config.workerId,
      messageId: message.id,
      jobName: envelope.name,
      attempt,
      error,
      willRetry,
    });
    this.config.logger?.debug?.(
      { messageId: message.id, jobName: envelope.name, attempt, willRetry, error },
      "job failed",
    );

    if (willRetry) {
      await this.retryMessage(message, envelope, error, internals.retryDelay);
    } else if (options.deadLetterQueue !== undefined) {
      await this.deadLetter(message, envelope, error);
    } else {
      // Exhausted without a DLQ: drop the poison work item instead of
      // looping forever. The "failed" event above is the audit trail.
      await this.ackQuietly(message);
    }
  }

  private async retryMessage(
    message: ReceivedMessage<unknown, TTypes>,
    envelope: QueueEnvelope,
    error: unknown,
    requestedDelayMs: number | undefined,
  ): Promise<void> {
    const { adapter, clock, serializer } = this.config;
    const nextAttempt = message.attempt + 1;
    const delayMs = requestedDelayMs ?? computeBackoff(this.config.options.backoff, message.attempt, error);

    this.events.emit("retrying", {
      workerId: this.config.workerId,
      messageId: message.id,
      jobName: envelope.name,
      attempt: message.attempt,
      nextAttempt,
      delayMs,
      error,
    });

    const retryEnvelope: QueueEnvelope = {
      ...envelope,
      attempt: nextAttempt,
      timestamp: clock.now(),
      metadata: { ...envelope.metadata, "x-retry-attempt": String(nextAttempt) },
    };
    try {
      await adapter.send(
        this.config.resolveRef(this.config.queueName),
        { envelope: retryEnvelope, body: serializer.encode(retryEnvelope) },
        { delayMs: delayMs > 0 ? delayMs : undefined },
      );
      await this.ackQuietly(message);
    } catch (sendError) {
      // Never lose the original while re-publishing fails.
      await this.nackQuietly(message, { requeue: true });
      this.emitError(sendError);
    }
  }

  private async deadLetter(
    message: ReceivedMessage<unknown, TTypes>,
    envelope: QueueEnvelope,
    error: unknown,
  ): Promise<void> {
    const { adapter, clock, serializer } = this.config;
    const deadLetterQueue = this.config.options.deadLetterQueue!;
    const deadLetterEnvelope: QueueEnvelope = {
      v: ENVELOPE_VERSION,
      id: createId(),
      name: envelope.name,
      version: envelope.version,
      payload: envelope.payload,
      timestamp: clock.now(),
      attempt: message.attempt,
      metadata: {
        ...envelope.metadata,
        "x-dead-letter": "true",
        "x-dead-letter-source": this.config.queueName,
        "x-dead-letter-error": error instanceof Error ? `${error.name}: ${error.message}` : String(error),
      },
      ...(envelope.trace !== undefined ? { trace: envelope.trace } : {}),
    };

    try {
      await adapter.send(
        this.config.resolveRef(deadLetterQueue),
        { envelope: deadLetterEnvelope, body: serializer.encode(deadLetterEnvelope) },
        {},
      );
      await this.ackQuietly(message);
      this.events.emit("dead-lettered", {
        workerId: this.config.workerId,
        messageId: message.id,
        jobName: envelope.name,
        attempt: message.attempt,
        deadLetterQueue,
        error,
      });
    } catch (dlqError) {
      await this.nackQuietly(message, { requeue: true });
      this.emitError(dlqError);
    }
  }

  private async handlePoisonMessage(message: ReceivedMessage<unknown, TTypes>): Promise<void> {
    const options = this.config.options;
    const policy: PoisonMessagePolicy =
      options.onPoisonMessage ?? (options.deadLetterQueue !== undefined ? "dead-letter" : "discard");

    if (policy === "retry") {
      await this.nackQuietly(message, { requeue: true });
      return;
    }
    if (policy === "dead-letter" && options.deadLetterQueue !== undefined) {
      const { adapter, clock, serializer } = this.config;
      const wrapper: QueueEnvelope = {
        v: ENVELOPE_VERSION,
        id: createId(),
        name: "queuekit.poison-message",
        payload: {
          originalMessageId: message.id,
          source: this.config.queueName,
          bodySnippet: snippet(message.body),
        },
        timestamp: clock.now(),
        attempt: message.attempt,
        metadata: {
          "x-dead-letter": "true",
          "x-dead-letter-source": this.config.queueName,
          "x-dead-letter-reason": "undecodable-body",
        },
      };
      try {
        await adapter.send(
          this.config.resolveRef(options.deadLetterQueue),
          { envelope: wrapper, body: serializer.encode(wrapper) },
          {},
        );
      } catch (error) {
        this.emitError(error);
        await this.nackQuietly(message, { requeue: true });
        return;
      }
      this.events.emit("dead-lettered", {
        workerId: this.config.workerId,
        messageId: message.id,
        jobName: "queuekit.poison-message",
        attempt: message.attempt,
        deadLetterQueue: options.deadLetterQueue,
        error: new QueueSerializationError("Message body could not be decoded"),
      });
    }
    await this.ackQuietly(message);
  }

  private async handleValidationFailure(
    message: ReceivedMessage<unknown, TTypes>,
    envelope: QueueEnvelope,
    error: unknown,
    internals: JobInternals,
  ): Promise<void> {
    const options = this.config.options;
    const policy: ValidationFailurePolicy =
      options.validationFailure ?? (options.deadLetterQueue !== undefined ? "dead-letter" : "discard");

    this.events.emit("failed", {
      workerId: this.config.workerId,
      messageId: message.id,
      jobName: envelope.name,
      attempt: message.attempt,
      error,
      willRetry: policy === "retry",
    });

    if (policy === "retry" || internals.settled === "retry") {
      await this.retryMessage(message, envelope, error, internals.retryDelay);
    } else if (policy === "dead-letter" && options.deadLetterQueue !== undefined) {
      await this.deadLetter(message, envelope, error);
    } else {
      await this.ackQuietly(message);
    }
  }

  private async ackQuietly(message: ReceivedMessage<unknown, TTypes>): Promise<void> {
    const ack = this.config.adapter.ack;
    if (ack === undefined) return;
    try {
      await ack.call(this.config.adapter, message);
    } catch (error) {
      this.emitError(error);
    }
  }

  private async nackQuietly(message: ReceivedMessage<unknown, TTypes>, options: { requeue?: boolean }): Promise<void> {
    const nack = this.config.adapter.nack;
    if (nack === undefined) return;
    try {
      await nack.call(this.config.adapter, message, { requeue: options.requeue });
    } catch (error) {
      this.emitError(error);
    }
  }

  // --------------------------------------------------------------- helpers

  private startVisibilityExtender(message: ReceivedMessage<unknown, TTypes>, signal: AbortSignal): { stop(): void } {
    const options = this.config.options;
    const extend = this.config.adapter.extendVisibility;
    const visibilityTimeoutMs = options.visibilityTimeoutMs;
    if (
      !options.autoExtendVisibility ||
      visibilityTimeoutMs === undefined ||
      !this.config.capabilities.visibilityTimeout.supported ||
      extend === undefined
    ) {
      return NOOP_EXTENDER;
    }
    const intervalMs = Math.max(1_000, Math.floor(visibilityTimeoutMs / 2));
    let stopped = false;
    void (async () => {
      while (!stopped && !signal.aborted) {
        try {
          await this.config.clock.delay(intervalMs, signal);
        } catch {
          return;
        }
        if (stopped || signal.aborted) return;
        try {
          await extend.call(this.config.adapter, message, visibilityTimeoutMs);
        } catch (error) {
          this.emitError(error);
          return;
        }
      }
    })();
    return {
      stop() {
        stopped = true;
      },
    };
  }

  private emitError(error: unknown): void {
    this.events.emit("error", { workerId: this.config.workerId, error });
  }
}

const NOOP_EXTENDER = {
  stop(): void {},
};

const ENCODER = new TextEncoder();
function snippet(body: string | Uint8Array): string {
  const text = typeof body === "string" ? body : new TextDecoder().decode(body);
  const bytes = ENCODER.encode(text);
  if (bytes.length <= POISON_SNIPPET_BYTES) return text;
  return `${new TextDecoder().decode(bytes.slice(0, POISON_SNIPPET_BYTES))}…[truncated]`;
}

function safeWhen(when: (error: unknown) => boolean, error: unknown): boolean {
  try {
    return when(error) === true;
  } catch {
    return false;
  }
}

function realDelay(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, ms);
    guardUnref(timer);
  });
}
