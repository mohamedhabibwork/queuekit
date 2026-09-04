import { parseDuration, type DurationInput } from "./duration";
import type { QueueLogger } from "./logger";

interface Closeable {
  close(options?: { timeout?: DurationInput | undefined; force?: boolean }): Promise<void>;
}

interface MinimalProcess {
  once?(signal: string, listener: () => void): unknown;
  removeListener?(signal: string, listener: () => void): unknown;
}

export interface GracefulShutdownOptions {
  workers?: readonly Closeable[];
  queues?: readonly Closeable[];
  /** Max time to wait for active jobs. Default "30s". */
  timeout?: DurationInput | undefined;
  signals?: readonly string[] | undefined;
  logger?: QueueLogger | undefined;
  /** Called after all resources are closed — e.g. to exit the process. */
  onShutdown?: (() => void) | undefined;
}

/**
 * Opt-in process signal wiring — core never attaches signal handlers on its
 * own. Returns a detach function that removes the listeners (and closes
 * everything) so tests can clean up.
 *
 * Works on Node, Bun and Deno (where `process` exists); it is a no-op
 * elsewhere.
 */
export function installGracefulShutdown(options: GracefulShutdownOptions): () => Promise<void> {
  const signals = options.signals ?? ["SIGINT", "SIGTERM"];
  const timeoutMs = options.timeout === undefined ? 30_000 : parseDuration(options.timeout);
  const processRef = (globalThis as { process?: MinimalProcess }).process;

  const shutdown = async (): Promise<void> => {
    options.logger?.info?.({ timeoutMs }, "graceful shutdown started");
    await Promise.allSettled([
      ...(options.workers ?? []).map((worker) => worker.close({ timeout: options.timeout })),
      ...(options.queues ?? []).map((queue) => queue.close({ timeout: options.timeout })),
    ]);
    options.logger?.info?.({}, "graceful shutdown complete");
    options.onShutdown?.();
  };

  if (processRef?.once === undefined || processRef.removeListener === undefined) {
    return shutdown;
  }

  const listeners = new Map<string, () => void>();
  for (const signal of signals) {
    const listener = (): void => {
      void shutdown();
    };
    listeners.set(signal, listener);
    processRef.once(signal, listener);
  }

  return async () => {
    for (const [signal, listener] of listeners) {
      processRef.removeListener!(signal, listener);
    }
    listeners.clear();
    await shutdown();
  };
}
