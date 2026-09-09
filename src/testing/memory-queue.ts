import { BaseQueueProvider } from '../core/base-provider.js';
import { createConsumer } from '../core/lifecycle.js';
import type { ConsumedMessage, ConsumerOptions, MessageContext, PublishOptions, PublishResult, QueueAcknowledgement, QueueCapabilities, QueueConsumer, QueueHandler, QueueMessage, QueueProvider } from '../core/types.js';

export interface MemoryMessage<TPayload = unknown> extends QueueMessage<TPayload> { readonly id: string; readonly timestamp: number; }
type Subscription = { readonly handler: QueueHandler<unknown, MemoryMessage, QueueAcknowledgement<undefined>>; readonly controller: AbortController; readonly consumer: QueueConsumer };

export class MemoryQueue extends BaseQueueProvider<'memory'> implements QueueProvider<'memory', never, MemoryMessage, MemoryMessage, QueueAcknowledgement<undefined>> {
  readonly name = 'memory' as const;
  readonly capabilities: QueueCapabilities = { kind: 'hybrid', publish: true, consume: true, batchPublish: true, delayed: true, retries: true, ack: true };
  readonly #messages = new Map<string, MemoryMessage[]>();
  readonly #subscriptions = new Map<string, Set<Subscription>>();
  #sequence = 0;
  #latency = 0;
  #nextError: Error | undefined;

  messages<TPayload = unknown>(destination: string): readonly MemoryMessage<TPayload>[] { return (this.#messages.get(destination) ?? []) as readonly MemoryMessage<TPayload>[]; }
  lastMessage<TPayload = unknown>(destination: string): MemoryMessage<TPayload> | undefined { return this.messages<TPayload>(destination).at(-1); }
  clear(destination?: string): void { if (destination) this.#messages.delete(destination); else this.#messages.clear(); }
  setLatency(milliseconds: number): void { this.#latency = Math.max(0, milliseconds); }
  failNext(error: Error = new Error('Memory queue requested failure.')): void { this.#nextError = error; }

  async publish<TPayload>(destination: string, message: QueueMessage<TPayload>, _options?: PublishOptions<never>): Promise<PublishResult<'memory', MemoryMessage<TPayload>>> {
    let result: PublishResult<'memory', MemoryMessage<TPayload>> | undefined;
    await this.operation({ provider: this.name, operation: 'publish', destination, traceId: message.traceId, correlationId: message.correlationId, metadata: message.metadata }, async () => {
      if (this.#nextError) { const error = this.#nextError; this.#nextError = undefined; throw error; }
      if (this.#latency) await new Promise((resolve) => setTimeout(resolve, this.#latency));
      const stored: MemoryMessage<TPayload> = { ...message, id: message.id ?? `memory-${++this.#sequence}`, timestamp: message.timestamp ?? Date.now() };
      const list = this.#messages.get(destination) ?? []; list.push(stored as MemoryMessage); this.#messages.set(destination, list);
      result = { ok: true, provider: this.name, messageId: stored.id, native: stored };
      for (const subscriber of this.#subscriptions.get(destination) ?? []) void this.#deliver(subscriber, stored);
    });
    return result!;
  }
  async publishMany<TPayload>(destination: string, messages: readonly QueueMessage<TPayload>[], options?: PublishOptions<never>): Promise<readonly PublishResult<'memory', MemoryMessage<TPayload>>[]> { return Promise.all(messages.map((message) => this.publish(destination, message, options))); }
  async consume<TPayload>(destination: string, handler: QueueHandler<TPayload, MemoryMessage<TPayload>, QueueAcknowledgement<undefined>>, _options?: ConsumerOptions): Promise<QueueConsumer> {
    const controller = new AbortController();
    let subscription: Subscription;
    const consumer = createConsumer({
      close: async () => { this.#subscriptions.get(destination)?.delete(subscription); controller.abort(); },
    });
    subscription = { handler: handler as QueueHandler<unknown, MemoryMessage, QueueAcknowledgement<undefined>>, controller, consumer };
    const subscriptions = this.#subscriptions.get(destination) ?? new Set<Subscription>(); subscriptions.add(subscription); this.#subscriptions.set(destination, subscriptions);
    return consumer;
  }
  async #deliver(subscription: Subscription, stored: MemoryMessage): Promise<void> {
    if (subscription.consumer.status !== 'running') return;
    const message: ConsumedMessage<unknown, MemoryMessage> = { ...stored, headers: stored.headers ?? {}, native: stored };
    const ack: QueueAcknowledgement<undefined> = { native: undefined, complete: async () => undefined, retry: async () => undefined, reject: async () => undefined };
    await subscription.handler({ message, ack, signal: subscription.controller.signal });
  }
  native(): this { return this; }
  async close(): Promise<void> { this.markClosed(); for (const set of this.#subscriptions.values()) for (const sub of set) await sub.consumer.close(); this.#subscriptions.clear(); }
}
export function createMemoryQueue(): MemoryQueue { return new MemoryQueue(); }
