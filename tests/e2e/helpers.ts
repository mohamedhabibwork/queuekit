import net from 'node:net';

/**
 * Brokers run from docker-compose.yml. Each suite resolves its endpoint from
 * the environment first so CI can point at real infrastructure, and falls
 * back to skipping when nothing is listening — `npm test` on a machine
 * without Docker still passes, it just runs the unit suites.
 */
export const REDIS_URL = process.env.QUEUEKIT_E2E_REDIS_URL ?? 'redis://127.0.0.1:6390';
export const VALKEY_URL = process.env.QUEUEKIT_E2E_VALKEY_URL ?? 'redis://127.0.0.1:6391';
export const RABBITMQ_URL = process.env.QUEUEKIT_E2E_RABBITMQ_URL ?? 'amqp://guest:guest@127.0.0.1:5673';
export const KAFKA_BROKER = process.env.QUEUEKIT_E2E_KAFKA_BROKER ?? '127.0.0.1:9092';
export const NATS_URL = process.env.QUEUEKIT_E2E_NATS_URL ?? 'nats://127.0.0.1:4222';
export const SQS_ENDPOINT = process.env.QUEUEKIT_E2E_SQS_ENDPOINT ?? 'http://127.0.0.1:4566';


export async function portOpen(port: number, host = '127.0.0.1', timeoutMs = 1_500): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    const settle = (open: boolean) => { socket.destroy(); resolve(open); };
    socket.setTimeout(timeoutMs);
    socket.once('connect', () => settle(true));
    socket.once('timeout', () => settle(false));
    socket.once('error', () => settle(false));
    socket.connect(port, host);
  });
}

export function endpointOf(url: string): { host: string; port: number } {
  const { hostname, port, protocol } = new URL(url);
  const defaultPorts: Record<string, number> = { 'redis:': 6379, 'amqp:': 5672, 'nats:': 4222, 'http:': 80, 'https:': 443 };
  return { host: hostname, port: Number(port) || (defaultPorts[protocol] ?? 80) };
}

/** Probe an endpoint once; when closed, say how to bring the stack up. */
export async function reachable(url: string, extraPort?: number): Promise<boolean> {
  if (extraPort !== undefined && !(await portOpen(extraPort))) return false;
  const { host, port } = endpointOf(url);
  return portOpen(port, host);
}

export function skipHint(url: string): string {
  return `[queuekit e2e] nothing listening at ${url} — start the stack with \`docker compose up -d\` or point the QUEUEKIT_E2E_* env vars at a live broker; skipping`;
}

/** Unique destination per run so reruns never collide with previous state. */
export function unique(prefix: string): string {
  const suffix = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
  return `${prefix}-${suffix}`;
}

export interface Deferred<T> { promise: Promise<T>; resolve: (value: T) => void; reject: (reason?: unknown) => void }
export function deferred<T = void>(): Deferred<T> {
  let resolve!: (value: T) => void; let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

/** Polls until the assertion passes or the deadline expires; surfaces the last error. */
export async function until(action: () => void | Promise<void>, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs; let lastError: unknown;
  while (Date.now() < deadline) {
    try { await action(); return; } catch (error) { lastError = error; await new Promise((resolve) => setTimeout(resolve, 100)); }
  }
  throw lastError instanceof Error ? lastError : new Error(`condition not met within ${timeoutMs}ms`);
}
