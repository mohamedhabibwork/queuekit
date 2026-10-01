import { QueueTimeoutError } from "./errors.js";

interface Closable {
  close(): Promise<void>;
}

export interface GracefulShutdownOptions {
  readonly consumers?: readonly Closable[];
  readonly queues?: readonly Closable[];
  /** Signals to listen for. Defaults to SIGINT and SIGTERM; pass [] to wire it yourself. */
  readonly signals?: readonly string[];
  /** Max milliseconds to wait for everything to close. */
  readonly timeout?: number;
  /** Exit the process after a signal-triggered shutdown. Defaults to true. */
  readonly exitOnSignal?: boolean;
  readonly onError?: (error: unknown) => void;
}

export interface GracefulShutdown {
  /** Closes consumers (draining in-flight work) then queues. Idempotent. */
  shutdown(): Promise<void>;
  /** Removes signal listeners. */
  dispose(): void;
}

type SignalProcess = {
  on(signal: string, listener: () => void): unknown;
  off(signal: string, listener: () => void): unknown;
  exit(code?: number): never;
};

export function gracefulShutdown(options: GracefulShutdownOptions): GracefulShutdown {
  const proc = (globalThis as { process?: SignalProcess }).process;
  const signals = options.signals ?? ["SIGINT", "SIGTERM"];
  let pending: Promise<void> | undefined;
  const closeAll = async () => {
    await Promise.all((options.consumers ?? []).map((consumer) => consumer.close()));
    await Promise.all((options.queues ?? []).map((queue) => queue.close()));
  };
  const withDeadline = (work: Promise<void>): Promise<void> => {
    if (options.timeout === undefined) return work;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(
        () => reject(new QueueTimeoutError(`Shutdown exceeded ${options.timeout}ms.`)),
        options.timeout,
      );
    });
    return Promise.race([work, deadline]).finally(() => clearTimeout(timer));
  };
  const shutdown = () => {
    pending ??= withDeadline(closeAll());
    return pending;
  };
  const onSignal = () => {
    const exit = options.exitOnSignal ?? true;
    void (async () => {
      try {
        await shutdown();
        if (exit) proc?.exit(0);
      } catch (error) {
        options.onError?.(error);
        if (exit) proc?.exit(1);
      }
    })();
  };
  for (const signal of signals) proc?.on(signal, onSignal);
  return {
    shutdown,
    dispose() {
      for (const signal of signals) proc?.off(signal, onSignal);
    },
  };
}
