import type { ConsumedMessage, QueueAcknowledgement } from "./types.js";
import type { ContextHandler } from "./handlers.js";

/** Records processed keys. Back it with Redis/SQL in production for cross-process dedupe. */
export interface IdempotencyStore {
  has(key: string): Promise<boolean> | boolean;
  set(key: string, ttlMs?: number): Promise<void> | void;
}

export function createMemoryIdempotencyStore(
  options: { readonly now?: () => number } = {},
): IdempotencyStore {
  const now = options.now ?? Date.now;
  const keys = new Map<string, number>();
  return {
    has(key) {
      const expiresAt = keys.get(key);
      if (expiresAt === undefined) return false;
      if (expiresAt <= now()) {
        keys.delete(key);
        return false;
      }
      return true;
    },
    set(key, ttlMs) {
      keys.set(key, ttlMs === undefined ? Number.POSITIVE_INFINITY : now() + ttlMs);
    },
  };
}

export interface WithIdempotencyOptions<TPayload> {
  readonly store?: IdempotencyStore;
  /** Derives the dedupe key; defaults to `message.id`. Return undefined to skip dedupe. */
  readonly key?: (message: ConsumedMessage<TPayload, unknown>) => string | undefined;
  readonly ttl?: number;
}

/**
 * Skips messages whose key was already processed successfully (acknowledging the
 * duplicate). Keys are recorded only after the handler succeeds, so failures retry.
 */
export function withIdempotency<TPayload, TNative, TAck extends QueueAcknowledgement>(
  handler: ContextHandler<TPayload, TNative, TAck>,
  options: WithIdempotencyOptions<TPayload> = {},
): ContextHandler<TPayload, TNative, TAck> {
  const store = options.store ?? createMemoryIdempotencyStore();
  const keyOf = options.key ?? ((message) => message.id);
  return async (context) => {
    const key = keyOf(context.message);
    if (key !== undefined && (await store.has(key))) {
      await context.ack.complete();
      return;
    }
    await handler(context);
    if (key !== undefined) await store.set(key, options.ttl);
  };
}
