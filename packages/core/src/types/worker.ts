import type { ProviderTypes } from "./provider";
import type { DurationInput } from "../duration";
import type { Unsubscribe } from "../events";

/**
 * The job context handed to worker handlers. `data` is inferred from the
 * job definition; `native` is the provider's delivery handle.
 */
export interface QueueJob<TData = unknown, TNative = unknown> {
  readonly id: string;
  readonly name: string;
  readonly version: number | undefined;
  /** Strongly typed when the worker was registered via a job definition. */
  readonly data: TData;
  /** 1-based delivery attempt. */
  readonly attempt: number;
  /** Total configured attempts, when a portable retry policy is set. */
  readonly maxAttempts: number | undefined;
  readonly metadata: Record<string, string>;
  readonly timestamp: number;
  /** Fires when the worker is force-closed or the worker signal aborts. */
  readonly signal: AbortSignal;
  /** Provider-native delivery handle (typed per provider). */
  readonly native: TNative;

  /** Settle the message as processed. Subsequent engine decisions are skipped. */
  ack(): Promise<void>;
  /** Drop the message without dead-lettering or retrying. */
  discard(): Promise<void>;
  /** Request a portable retry, optionally after a delay. */
  retry(options?: { delay?: number | `${number}ms` | `${number}s` | `${number}m` | `${number}h` | `${number}d` }): Promise<void>;
}

export type JobSettlement = "ack" | "discard" | "retry";

/** Internal view the engine uses to coordinate settlement intents. */
export interface JobInternals {
  settled: JobSettlement | null;
  retryDelay: number | undefined;
}

export type JobHandler<TData = unknown, TResult = unknown, TNative = unknown> = (
  job: QueueJob<TData, TNative>,
) => Promise<TResult> | TResult;

/** Handle to a running worker. */
export interface QueueWorker<TTypes extends ProviderTypes = ProviderTypes> {
  readonly id: string;
  readonly queue: string;
  readonly running: boolean;
  readonly paused: boolean;

  /** Begin consuming (relevant when the worker was created with `autoStart: false`). */
  start(): void;
  pause(): Promise<void>;
  resume(): Promise<void>;
  /**
   * Graceful shutdown: stop fetching, let active jobs finish, release
   * provider resources. `force` aborts active handlers immediately.
   */
  close(options?: { timeout?: DurationInput; force?: boolean }): Promise<void>;

  on<K extends keyof WorkerEvents<TTypes> & string>(
    event: K,
    handler: (payload: WorkerEvents<TTypes>[K]) => void | Promise<void>,
  ): Unsubscribe;
}

/** Events emitted by workers. Each carries provider-native context where useful. */
export interface WorkerEvents<TTypes extends ProviderTypes = ProviderTypes> {
  started: { workerId: string };
  stopped: { workerId: string };
  paused: { workerId: string };
  resumed: { workerId: string };
  received: {
    workerId: string;
    messageId: string;
    jobName: string;
    attempt: number;
    native: TTypes["nativeMessage"];
  };
  completed: {
    workerId: string;
    messageId: string;
    jobName: string;
    attempt: number;
    durationMs: number;
    result: unknown;
  };
  failed: {
    workerId: string;
    messageId: string;
    jobName: string;
    attempt: number;
    error: unknown;
    willRetry: boolean;
  };
  retrying: {
    workerId: string;
    messageId: string;
    jobName: string;
    attempt: number;
    nextAttempt: number;
    delayMs: number;
    error: unknown;
  };
  "dead-lettered": {
    workerId: string;
    messageId: string;
    jobName: string;
    attempt: number;
    deadLetterQueue: string;
    error: unknown;
  };
  error: { workerId: string; error: unknown };
}
