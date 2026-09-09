import type { QueueAcknowledgement, QueueHandler, QueueProvider } from './core/types.js';

export type RegistryPayload<T> = T extends { readonly payload: infer TPayload } ? TPayload : T;
export type RegistryKeys<T> = Extract<keyof T, string>;

export function createTypedQueue<TRegistry extends Record<string, unknown>>(provider: QueueProvider) {
  return {
    publish<TKey extends RegistryKeys<TRegistry>>(destination: TKey, payload: RegistryPayload<TRegistry[TKey]>) {
      return provider.publish(destination, { payload });
    },
    consume<TKey extends RegistryKeys<TRegistry>>(destination: TKey, handler: QueueHandler<RegistryPayload<TRegistry[TKey]>, unknown, QueueAcknowledgement>) {
      return provider.consume(destination, handler);
    },
  };
}
