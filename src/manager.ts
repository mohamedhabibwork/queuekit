import { QueueConfigError } from "./core/errors.js";
import type { QueueHealth, QueueProvider } from "./core/types.js";
import type { BuiltInQueueConfig } from "./config.js";
import { createQueue, type QueueForConfig } from "./factory.js";
import { noopLogger, toError, type KitLogger } from "./core/logger.js";

type ProviderMap<T extends Record<string, BuiltInQueueConfig>> = {
  [TKey in keyof T]: QueueForConfig<T[TKey]>;
};
export interface QueueManager<TProviders extends Record<string, BuiltInQueueConfig>> {
  provider<TKey extends keyof TProviders>(name: TKey): Promise<ProviderMap<TProviders>[TKey]>;
  default(): Promise<ProviderMap<TProviders>[keyof TProviders]>;
  warmup(): Promise<void>;
  health(): Promise<{ [TKey in keyof TProviders]: QueueHealth }>;
  close(): Promise<void>;
}
export function createQueueManager<
  const TProviders extends Record<string, BuiltInQueueConfig>,
>(options: {
  readonly providers: TProviders;
  readonly default?: keyof TProviders;
  /** Optional logger (e.g. a loggerkit `Logger`) for lifecycle events. */
  readonly logger?: KitLogger;
}): QueueManager<TProviders> {
  const logger = options.logger ?? noopLogger;
  const providers = new Map<string, Promise<QueueProvider>>();
  const provider = async <TKey extends keyof TProviders>(
    name: TKey,
  ): Promise<ProviderMap<TProviders>[TKey]> => {
    const config = options.providers[name];
    if (!config) throw new QueueConfigError(`Unknown queue provider alias: ${String(name)}.`);
    let instance = providers.get(String(name));
    if (!instance) {
      const key = String(name);
      instance = createQueue(config).then(
        (created) => {
          logger.debug("queuekit: provider created", { provider: key });
          return created;
        },
        (error: unknown) => {
          providers.delete(key);
          logger.error(`queuekit: failed to create provider "${key}"`, toError(error));
          throw error;
        },
      );
      providers.set(String(name), instance);
    }
    return instance as Promise<ProviderMap<TProviders>[TKey]>;
  };
  return {
    provider,
    async default() {
      if (!options.default) throw new QueueConfigError("No default queue provider is configured.");
      return provider(options.default);
    },
    async warmup() {
      await Promise.all(Object.keys(options.providers).map((name) => provider(name)));
    },
    async health() {
      const entries = await Promise.all(
        Object.keys(options.providers).map(
          async (name) =>
            [
              name,
              (await (await provider(name)).health?.()) ?? { ok: true, provider: name },
            ] as const,
        ),
      );
      return Object.fromEntries(entries) as { [TKey in keyof TProviders]: QueueHealth };
    },
    async close() {
      await Promise.all([...providers.values()].map(async (item) => (await item).close()));
      providers.clear();
      logger.debug("queuekit: manager closed");
    },
  };
}
