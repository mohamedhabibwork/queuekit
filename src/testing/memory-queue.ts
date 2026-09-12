import { BaseQueueProvider } from '../core/base-provider.js';
import { QueueClosedError } from '../core/errors.js';
import { createConsumer } from '../core/lifecycle.js';
import type { ConsumedMessage, ConsumerOptions, PublishOptions, PublishResult, QueueAcknowledgement, QueueCapabilities, QueueConsumer, QueueHandler, QueueProvider, QueueMessage } from '../core/types.js';
import type { MemoryConfig } from '../config.js';

export interface MemoryMessage<TPayload = unknown> extends QueueMessage<TPayload> { readonly id: string; readonly timestamp: number; }
export interface MemoryFailure { readonly destination: string; readonly message: MemoryMessage; readonly error: unknown; }
type PendingEntry = { readonly message: MemoryMessage; attempt: number; availableAt: number; settled: boolean };
type Subscription = { readonly handler: QueueHandler<unknown, MemoryMessage, QueueAcknowledgement<MemoryMessage>>; readonly controller: AbortController; readonly consumer: QueueConsumer; readonly autoAck: boolean; readonly concurrency: number; paused: boolean; active: number };

/**
 * In-memory fake driver for tests. Messages are stored per destination and
 * delivered to consumers in FIFO order; messages published before a consumer
 * attaches stay queued and are delivered when it does. Delivery is
 * deterministic: handlers are invoked before `publish`/`consume` resolve and
 * `waitUntilIdle()` awaits all in-flight handler work.
 */
export class MemoryQueue extends BaseQueueProvider<'memory'> implements QueueProvider<'memory', never, MemoryMessage, MemoryMessage, QueueAcknowledgement<MemoryMessage>> {
  readonly name = 'memory' as const;
  readonly capabilities: QueueCapabilities = { kind: 'hybrid', publish: true, consume: true, batchPublish: true, delayed: true, retries: true, ack: true, deadLetter: true };
  readonly #messages = new Map<string, MemoryMessage[]>();
  readonly #pending = new Map<string, PendingEntry[]>();
  readonly #outstanding = new Map<string, PendingEntry[]>();
  readonly #dead = new Map<string, MemoryMessage[]>();
  readonly #acknowledged = new Map<string, MemoryMessage[]>();
  readonly #failures = new Map<string, MemoryFailure[]>();
  readonly #subscriptions = new Map<string, Set<Subscription>>();
  #sequence = 0;
  #latency = 0;
  #maxAttempts = 3;
  #inFlight = 0;
  #paused = false;
  #stopping = false;
  #nextError: Error | undefined;
  #idleResolvers: Array<() => void> = [];
  #timer: ReturnType<typeof setTimeout> | undefined;
  #timerAt: number | undefined;

  messages<TPayload = unknown>(destination: string): readonly MemoryMessage<TPayload>[] { return (this.#messages.get(destination) ?? []) as readonly MemoryMessage<TPayload>[]; }
  lastMessage<TPayload = unknown>(destination: string): MemoryMessage<TPayload> | undefined { return this.messages<TPayload>(destination).at(-1); }
  /** Messages published but not yet settled: queued, in-flight, or awaiting acknowledgement. */
  pending(destination: string): readonly MemoryMessage[] { return [...(this.#outstanding.get(destination) ?? []), ...(this.#pending.get(destination) ?? [])].map((entry) => entry.message); }
  deadLetters(destination: string): readonly MemoryMessage[] { return this.#dead.get(destination) ?? []; }
  acknowledged(destination: string): readonly MemoryMessage[] { return this.#acknowledged.get(destination) ?? []; }
  failures(destination: string): readonly MemoryFailure[] { return this.#failures.get(destination) ?? []; }
  /** Discards all stored state (messages, queues, acknowledgements, failures). */
  clear(destination?: string): void {
    const targets = destination ? [destination] : [...new Set([...this.#messages.keys(), ...this.#pending.keys(), ...this.#outstanding.keys(), ...this.#dead.keys(), ...this.#acknowledged.keys(), ...this.#failures.keys()])];
    for (const key of targets) { this.#messages.delete(key); this.#pending.delete(key); this.#outstanding.delete(key); this.#dead.delete(key); this.#acknowledged.delete(key); this.#failures.delete(key); }
    if (!destination) this.#sequence = 0;
  }

  setLatency(milliseconds: number): void { this.#latency = Math.max(0, milliseconds); }
  /** Maximum delivery attempts before a throwing handler dead-letters its message. Defaults to 3. */
  setMaxAttempts(attempts: number): void { this.#maxAttempts = Math.max(1, attempts); }
  failNext(error: Error = new Error('Memory queue requested failure.')): void { this.#nextError = error; }
  /** Holds delivery globally; queued messages are delivered again after `resume()`. */
  pause(): void { this.#paused = true; }
  resume(): void { this.#paused = false; this.#pump(); }
  /** Makes every queued (including delayed) message immediately available, then waits for delivery to settle. */
  async flush(): Promise<void> {
    const now = Date.now();
    for (const entries of this.#pending.values()) for (const entry of entries) entry.availableAt = now;
    this.#pump();
    await this.waitUntilIdle();
  }
  /** Resolves once no handler work is in flight and no immediately-deliverable message is queued. */
  async waitUntilIdle(): Promise<void> {
    for (;;) {
      this.#pump();
      if (this.#inFlight === 0 && !this.#hasDueWork()) return;
      if (this.#inFlight > 0) await new Promise<void>((resolve) => this.#idleResolvers.push(resolve));
      else await new Promise<void>((resolve) => setTimeout(resolve, 0));
    }
  }

  async publish<TPayload>(destination: string, message: QueueMessage<TPayload>, options?: PublishOptions<never>): Promise<PublishResult<'memory', MemoryMessage<TPayload>>> {
    let result: PublishResult<'memory', MemoryMessage<TPayload>> | undefined;
    await this.operation({ provider: this.name, operation: 'publish', destination, traceId: message.traceId, correlationId: message.correlationId, metadata: message.metadata }, async () => {
      if (this.#nextError) { const error = this.#nextError; this.#nextError = undefined; throw error; }
      if (this.#latency) await new Promise((resolve) => setTimeout(resolve, this.#latency));
      const stored: MemoryMessage<TPayload> = { ...message, id: message.id ?? `memory-${++this.#sequence}`, timestamp: message.timestamp ?? Date.now() };
      const list = this.#messages.get(destination) ?? []; list.push(stored as MemoryMessage); this.#messages.set(destination, list);
      const availableAt = options?.scheduleAt ? options.scheduleAt.getTime() : Date.now() + (options?.delay ?? 0);
      const entries = this.#pending.get(destination) ?? []; entries.push({ message: stored as MemoryMessage, attempt: 1, availableAt, settled: false }); this.#pending.set(destination, entries);
      result = { ok: true, provider: this.name, messageId: stored.id, native: stored };
      this.#pump();
    });
    return result!;
  }
  async publishMany<TPayload>(destination: string, messages: readonly QueueMessage<TPayload>[], options?: PublishOptions<never>): Promise<readonly PublishResult<'memory', MemoryMessage<TPayload>>[]> {
    const results: PublishResult<'memory', MemoryMessage<TPayload>>[] = [];
    for (const message of messages) results.push(await this.publish(destination, message, options));
    return results;
  }
  async consume<TPayload>(destination: string, handler: QueueHandler<TPayload, MemoryMessage<TPayload>, QueueAcknowledgement<MemoryMessage>>, options?: ConsumerOptions): Promise<QueueConsumer> {
    if (this.#stopping) throw new QueueClosedError(`The ${this.name} provider is closed.`, { provider: this.name, operation: 'consume', destination });
    const controller = new AbortController();
    let subscription: Subscription;
    const consumer = createConsumer({
      pause: async () => { subscription.paused = true; },
      resume: async () => { subscription.paused = false; this.#pump(); },
      close: async () => { controller.abort(); this.#subscriptions.get(destination)?.delete(subscription); },
    });
    subscription = { handler: handler as QueueHandler<unknown, MemoryMessage, QueueAcknowledgement<MemoryMessage>>, controller, consumer, autoAck: options?.autoAck ?? true, concurrency: Math.max(1, options?.concurrency ?? 1), paused: false, active: 0 };
    const subscriptions = this.#subscriptions.get(destination) ?? new Set<Subscription>(); subscriptions.add(subscription); this.#subscriptions.set(destination, subscriptions);
    this.#pump();
    return consumer;
  }
  native(): this { return this; }
  async close(): Promise<void> {
    this.#stopping = true; this.markClosed(); this.#paused = false;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined; this.#timerAt = undefined;
    const subscriptions = [...this.#subscriptions.values()].flatMap((set) => [...set]);
    this.#subscriptions.clear();
    await Promise.all(subscriptions.map((subscription) => subscription.consumer.close()));
  }

  #runnable(destination: string): Subscription[] { return this.#paused ? [] : [...(this.#subscriptions.get(destination) ?? [])].filter((subscription) => !subscription.paused && subscription.consumer.status === 'running' && subscription.active < subscription.concurrency); }
  #hasDueWork(): boolean {
    if (this.#paused) return false;
    const now = Date.now();
    for (const [destination, entries] of this.#pending) { if (!entries.some((entry) => entry.availableAt <= now)) continue; if (this.#runnable(destination).length > 0) return true; }
    return false;
  }
  #pump(): void {
    if (this.#paused || this.#stopping) return;
    const now = Date.now();
    for (const [destination, entries] of this.#pending) {
      if (entries.length === 0) continue;
      for (;;) {
        const entry = entries[0];
        if (!entry || entry.availableAt > now) break;
        const subscription = this.#runnable(destination).at(0);
        if (!subscription) break;
        entries.shift();
        this.#deliver(subscription, destination, entry);
      }
    }
    this.#scheduleTimer();
  }
  #scheduleTimer(): void {
    let next: number | undefined;
    for (const entries of this.#pending.values()) for (const entry of entries) if (next === undefined || entry.availableAt < next) next = entry.availableAt;
    if (next === undefined || (this.#timer && this.#timerAt !== undefined && this.#timerAt <= next)) return;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timerAt = next;
    this.#timer = setTimeout(() => { this.#timer = undefined; this.#timerAt = undefined; this.#pump(); }, Math.max(0, next - Date.now()));
    (this.#timer as { unref?: () => void }).unref?.();
  }
  #requeue(destination: string, entry: PendingEntry, toFront = true): void {
    const entries = this.#pending.get(destination) ?? [];
    if (toFront) entries.unshift(entry); else entries.push(entry);
    this.#pending.set(destination, entries);
    this.#pump();
  }
  #recordFailure(destination: string, failure: MemoryFailure): void { const list = this.#failures.get(destination) ?? []; list.push(failure); this.#failures.set(destination, list); }
  #takeOutstanding(destination: string, entry: PendingEntry): void {
    const outstanding = this.#outstanding.get(destination);
    if (!outstanding) return;
    const index = outstanding.indexOf(entry);
    if (index >= 0) outstanding.splice(index, 1);
    if (outstanding.length === 0) this.#outstanding.delete(destination);
  }
  #deliver(subscription: Subscription, destination: string, entry: PendingEntry): void {
    const outstanding = this.#outstanding.get(destination) ?? []; outstanding.push(entry); this.#outstanding.set(destination, outstanding);
    subscription.active++;
    this.#inFlight++;
    const finish = () => {
      subscription.active--; this.#inFlight--;
      if (this.#inFlight === 0) { const resolvers = this.#idleResolvers; this.#idleResolvers = []; for (const resolve of resolvers) resolve(); }
      this.#pump();
    };
    const settle = (): boolean => { if (entry.settled) return false; entry.settled = true; return true; };
    const ack: QueueAcknowledgement<MemoryMessage> = {
      native: entry.message,
      complete: async () => {
        if (!settle()) return;
        this.#takeOutstanding(destination, entry);
        const list = this.#acknowledged.get(destination) ?? []; list.push(entry.message); this.#acknowledged.set(destination, list);
      },
      retry: async (options) => {
        if (!settle()) return;
        this.#takeOutstanding(destination, entry);
        this.#requeue(destination, { message: entry.message, attempt: entry.attempt + 1, availableAt: Date.now() + (options?.delay ?? 0), settled: false }, false);
      },
      reject: async (options) => {
        if (!settle()) return;
        this.#takeOutstanding(destination, entry);
        if (options?.requeue) this.#requeue(destination, { message: entry.message, attempt: entry.attempt, availableAt: Date.now(), settled: false });
        else { const list = this.#dead.get(destination) ?? []; list.push(entry.message); this.#dead.set(destination, list); }
      },
    };
    const consumed: ConsumedMessage<unknown, MemoryMessage> = { ...entry.message, headers: entry.message.headers ?? {}, attempt: entry.attempt, native: entry.message };
    void (async () => {
      try {
        await subscription.handler({ message: consumed, ack, signal: subscription.controller.signal });
        if (subscription.autoAck && !entry.settled) await ack.complete();
      } catch (error) {
        this.#recordFailure(destination, { destination, message: entry.message, error });
        if (!entry.settled) {
          this.#takeOutstanding(destination, entry);
          if (entry.attempt >= this.#maxAttempts) { entry.settled = true; const list = this.#dead.get(destination) ?? []; list.push(entry.message); this.#dead.set(destination, list); }
          else this.#requeue(destination, { message: entry.message, attempt: entry.attempt + 1, availableAt: Date.now(), settled: false });
        }
      } finally { finish(); }
    })();
  }
}
export function createMemoryQueue(config?: MemoryConfig): MemoryQueue {
  const queue = new MemoryQueue();
  if (config?.latency) queue.setLatency(config.latency);
  return queue;
}
/** Alias of {@link createMemoryQueue}; both names return the same in-memory fake driver. */
export function createFakeQueue(config?: MemoryConfig): MemoryQueue { return createMemoryQueue(config); }
