import type { DurationInput } from "./duration";
import { parseDuration } from "./duration";
import { QueueTimeoutError } from "./errors";
import type { QueueEnvelope } from "./envelope";
import type { MutableSendOptions } from "./types/options";
import type { ProviderTypes } from "./types/provider";
import type { ProducerMiddleware } from "./middleware";
import type { QueueEvents, QueueKit } from "./queue";
import type { WorkerEvents, QueueWorker } from "./types/worker";

/**
 * Recording middleware-based test helper: captures every send that passes
 * through the queue without mocking any provider SDK.
 *
 * ```ts
 * const capture = captureJobs(queue);
 * await service.checkout();
 * expect(capture.jobs.map((j) => j.name)).toContain("order.created");
 * ```
 */
export interface CapturedJob<T = unknown> {
  queue: string;
  name: string;
  payload: T;
  envelope: QueueEnvelope<T>;
  options: MutableSendOptions;
  at: number;
}

export interface CaptureHandle {
  readonly jobs: readonly CapturedJob[];
  clear(): void;
  /** Stop recording (middleware cannot be removed once registered). */
  dispose(): void;
}

export function captureJobs<TTypes extends ProviderTypes>(queue: QueueKit<TTypes>): CaptureHandle {
  const jobs: CapturedJob[] = [];
  let active = true;
  const middleware: ProducerMiddleware<TTypes> = async (context, next) => {
    if (active) {
      jobs.push({
        queue: context.queue.name,
        name: context.envelope.name,
        payload: context.envelope.payload,
        envelope: context.envelope,
        options: context.options,
        at: Date.now(),
      });
    }
    return next();
  };
  queue.use(middleware);
  return {
    get jobs(): readonly CapturedJob[] {
      return jobs;
    },
    clear(): void {
      jobs.length = 0;
    },
    dispose(): void {
      active = false;
    },
  };
}

export interface WaitForOptions<TPayload> {
  timeout?: DurationInput | undefined;
  filter?: (payload: TPayload) => boolean | undefined;
}

/** Wait for the next matching queue event (uses real time, not the queue clock). */
export function waitForQueueEvent<TTypes extends ProviderTypes, K extends keyof QueueEvents<TTypes> & string>(
  queue: QueueKit<TTypes>,
  event: K,
  options: WaitForOptions<QueueEvents<TTypes>[K]> = {},
): Promise<QueueEvents<TTypes>[K]> {
  return waitForEventImplementation<QueueEvents<TTypes>[K]>({
    timeout: options.timeout,
    filter: options.filter,
    subscribe: (handler) => queue.on(event, handler),
  });
}

/** Wait for the next matching worker event (uses real time, not the queue clock). */
export function waitForWorkerEvent<TTypes extends ProviderTypes, K extends keyof WorkerEvents<TTypes> & string>(
  worker: QueueWorker<TTypes>,
  event: K,
  options: WaitForOptions<WorkerEvents<TTypes>[K]> = {},
): Promise<WorkerEvents<TTypes>[K]> {
  return waitForEventImplementation<WorkerEvents<TTypes>[K]>({
    timeout: options.timeout,
    filter: options.filter,
    subscribe: (handler) => worker.on(event, handler),
  });
}

function waitForEventImplementation<TPayload>(config: {
  timeout: DurationInput | undefined;
  filter: ((payload: TPayload) => boolean | undefined) | undefined;
  subscribe: (handler: (payload: TPayload) => void | Promise<void>) => () => void;
}): Promise<TPayload> {
  const timeoutMs = config.timeout === undefined ? 5_000 : parseDuration(config.timeout);
  return new Promise<TPayload>((resolve, reject) => {
    let settled = false;
    const finish = (action: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      unsubscribe();
      action();
    };
    const timer = setTimeout(() => {
      finish(() => reject(new QueueTimeoutError(`Timed out after ${timeoutMs}ms waiting for event`)));
    }, timeoutMs);

    const unsubscribe = config.subscribe((payload) => {
      if (config.filter !== undefined && config.filter(payload) !== true) return;
      finish(() => resolve(payload));
    });
  });
}
