/**
 * Structured logger hook. Queue Kit never depends on a logging library —
 * pass Pino, Winston, console or anything that satisfies this shape.
 * All methods are optional so partial loggers work.
 */
export interface QueueLogger {
  debug?: ((data: unknown, message?: string) => void) | undefined;
  info?: ((data: unknown, message?: string) => void) | undefined;
  warn?: ((data: unknown, message?: string) => void) | undefined;
  error?: ((data: unknown, message?: string) => void) | undefined;
}

export type LogLevel = "debug" | "info" | "warn" | "error" | "silent";

const LEVEL_WEIGHT: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
  silent: 100,
};

/** Convenience console logger, useful for examples and local development. */
export function createConsoleLogger(level: LogLevel = "info"): QueueLogger {
  const threshold = LEVEL_WEIGHT[level];
  return {
    debug: threshold <= LEVEL_WEIGHT.debug ? (data, message) => console.debug(message ?? "", data) : undefined,
    info: threshold <= LEVEL_WEIGHT.info ? (data, message) => console.info(message ?? "", data) : undefined,
    warn: threshold <= LEVEL_WEIGHT.warn ? (data, message) => console.warn(message ?? "", data) : undefined,
    error: threshold <= LEVEL_WEIGHT.error ? (data, message) => console.error(message ?? "", data) : undefined,
  };
}
