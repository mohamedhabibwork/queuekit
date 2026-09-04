import type { ProviderTypes } from "./types/provider";
import type { QueueKit } from "./queue";

type AnyQueue = QueueKit<ProviderTypes>;

/**
 * Named queue registry. The typed form preserves each queue's provider type:
 *
 * ```ts
 * const registry = createQueueRegistry({ emails: emailQueue, orders: orderQueue });
 * registry.get("emails"); // exact QueueKit type of emailQueue
 * ```
 */
export interface QueueRegistry<TMap extends Record<string, AnyQueue> = Record<string, AnyQueue>> {
  get<K extends keyof TMap & string>(name: K): TMap[K];
  has(name: string): boolean;
  names(): string[];
  /** Close every registered queue (and their workers). */
  closeAll(options?: { timeout?: number }): Promise<void>;
}

export function createQueueRegistry<const TMap extends Record<string, AnyQueue> = Record<string, AnyQueue>>(
  queues?: TMap,
): QueueRegistry<TMap> {
  const map = new Map<string, AnyQueue>(Object.entries(queues ?? ({} as TMap)));

  return {
    get(name: string): AnyQueue {
      const queue = map.get(name);
      if (queue === undefined) {
        throw new Error(`No queue registered under "${name}". Registered: ${[...map.keys()].join(", ") || "<none>"}`);
      }
      return queue;
    },
    has(name: string): boolean {
      return map.has(name);
    },
    names(): string[] {
      return [...map.keys()];
    },
    async closeAll(options?: { timeout?: number }): Promise<void> {
      await Promise.allSettled([...map.values()].map((queue) => queue.close({ timeout: options?.timeout })));
    },
  } as QueueRegistry<TMap>;
}
