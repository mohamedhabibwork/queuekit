import { QueueConfigError } from "./errors.js";
import type { PublishOptions, QueueMessage, QueueProvider } from "./types.js";

export interface RecurringOptions<TNative = unknown> {
  /** Interval in milliseconds between publishes. */
  readonly every: number;
  /** Publish once immediately instead of waiting for the first interval. */
  readonly immediate?: boolean;
  /** Stop after this many successful publishes. */
  readonly limit?: number;
  readonly publishOptions?: PublishOptions<TNative>;
  readonly onError?: (error: unknown) => void;
}

export interface RecurringJob {
  readonly runs: number;
  stop(): void;
}

type Publisher<TNative> = Pick<QueueProvider<string, TNative>, "publish">;

/**
 * Publishes `message` every `every` ms from this process. For cron-grade or
 * multi-instance schedules prefer native features (BullMQ `repeat`, SQS + EventBridge).
 */
export function scheduleRecurring<TPayload, TNative = unknown>(
  provider: Publisher<TNative>,
  destination: string,
  message: QueueMessage<TPayload> | (() => QueueMessage<TPayload>),
  options: RecurringOptions<TNative>,
): RecurringJob {
  if (!Number.isFinite(options.every) || options.every <= 0)
    throw new QueueConfigError("scheduleRecurring: `every` must be a positive number.");
  let runs = 0;
  let stopped = false;
  const tick = async () => {
    if (stopped) return;
    try {
      const next = typeof message === "function" ? message() : message;
      await provider.publish(destination, next, options.publishOptions);
      runs++;
      if (options.limit !== undefined && runs >= options.limit) job.stop();
    } catch (error) {
      options.onError?.(error);
    }
  };
  const timer = setInterval(() => void tick(), options.every);
  (timer as { unref?: () => void }).unref?.();
  const job: RecurringJob = {
    get runs() {
      return runs;
    },
    stop() {
      stopped = true;
      clearInterval(timer);
    },
  };
  if (options.immediate) void tick();
  return job;
}
