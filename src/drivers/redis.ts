import { BaseQueueProvider } from '../core/base-provider.js';
import { asText, jsonCodec } from '../core/codec.js';
import { createConsumer } from '../core/lifecycle.js';
import { loadOptional } from '../core/load-optional.js';
import type { RedisConfig } from '../config.js';
import type { ConsumerOptions, PublishOptions, PublishResult, QueueAcknowledgement, QueueCapabilities, QueueConsumer, QueueHandler, QueueMessage } from '../core/types.js';

export interface RedisPublishOptions { readonly maxLength?: number; readonly approximate?: boolean; }
export interface RedisConsumerOptions { readonly blockMs?: number; readonly count?: number; }
export type RedisAcknowledgement = QueueAcknowledgement<unknown>;
type RedisClient = Awaited<ReturnType<typeof import('redis')['createClient']>>;
type RedisStreamMessage<TPayload> = QueueMessage<TPayload> & { readonly attempt?: number };

/** The Redis commands needed to settle one Streams entry. Kept small for deterministic tests. */
export interface RedisStreamCommandClient {
  sendCommand<T = unknown>(command: string[]): Promise<T>;
}

/**
 * Redis Streams has no native retry or dead-letter command. Retrying means
 * adding a new entry before acknowledging the old pending one; if adding fails,
 * the old entry remains pending rather than being lost.
 */
export function createRedisStreamAcknowledgement<TPayload>(
  client: RedisStreamCommandClient,
  destination: string,
  group: string,
  entryId: string,
  message: RedisStreamMessage<TPayload>,
  attempt: number,
): RedisAcknowledgement {
  let settled = false;
  const complete = async (): Promise<void> => {
    if (settled) return;
    await client.sendCommand(['XACK', destination, group, entryId]);
    settled = true;
  };
  const retry = async (): Promise<void> => {
    if (settled) return;
    await client.sendCommand([
      'XADD',
      destination,
      '*',
      'data',
      JSON.stringify({ ...message, attempt: attempt + 1 }),
    ]);
    await complete();
  };
  return {
    native: undefined,
    complete,
    retry,
    reject: async (options) => {
      if (options?.requeue) await retry();
      else await complete();
    },
  };
}
export class RedisProvider extends BaseQueueProvider<'redis'> {
  readonly name = 'redis' as const;
  readonly capabilities: QueueCapabilities;
  #sdk: typeof import('redis') | undefined; #client: RedisClient | undefined; readonly #subscribers = new Set<RedisClient>();
  constructor(readonly config: RedisConfig) { super(); this.capabilities = config.mode === 'streams' ? { kind: 'stream', publish: true, consume: true, ack: true, replay: true, consumerGroups: true, ordering: true } : { kind: 'pubsub', publish: true, consume: true }; }
  async #module(): Promise<typeof import('redis')> { return this.#sdk ??= await loadOptional<typeof import('redis')>('redis', this.name); }
  async #getClient(): Promise<RedisClient> { if (this.#client) return this.#client; this.#client = (await this.#module()).createClient({ url: this.config.url }); this.#client.on('error', () => undefined); await this.#client.connect(); return this.#client; }
  async publish<TPayload>(destination: string, message: QueueMessage<TPayload>, options?: PublishOptions<RedisPublishOptions>): Promise<PublishResult<'redis', number | string>> {
    let result: PublishResult<'redis', number | string> | undefined; await this.operation({ provider: this.name, operation: 'publish', destination, traceId: message.traceId, correlationId: message.correlationId, metadata: message.metadata }, async () => { const client = await this.#getClient(); const encoded = asText(jsonCodec.encode(message)); const native = this.config.mode === 'pubsub' ? await client.publish(destination, encoded) : await client.sendCommand<string>(['XADD', destination, ...(options?.native?.maxLength ? ['MAXLEN', options.native.approximate === false ? '=' : '~', String(options.native.maxLength)] : []), '*', 'data', encoded]); result = { ok: true, provider: this.name, messageId: typeof native === 'string' ? native : message.id, native }; }); return result!;
  }
  async consume<TPayload>(destination: string, handler: QueueHandler<TPayload, unknown, RedisAcknowledgement>, options?: ConsumerOptions<RedisConsumerOptions>): Promise<QueueConsumer> { return this.config.mode === 'pubsub' ? this.#consumePubsub(destination, handler, options) : this.#consumeStream(destination, handler, options); }
  async #consumePubsub<TPayload>(destination: string, handler: QueueHandler<TPayload, unknown, RedisAcknowledgement>, _options?: ConsumerOptions<RedisConsumerOptions>): Promise<QueueConsumer> { const subscriber = (await this.#getClient()).duplicate(); subscriber.on('error', () => undefined); await subscriber.connect(); this.#subscribers.add(subscriber); const controller = new AbortController(); await subscriber.subscribe(destination, async (raw) => { const envelope = jsonCodec.decode(raw) as QueueMessage<TPayload>; const ack: RedisAcknowledgement = { native: undefined, complete: async () => undefined }; await handler({ message: { ...envelope, headers: envelope.headers ?? {}, native: raw }, ack, signal: controller.signal }); }); return createConsumer({ close: async () => { controller.abort(); this.#subscribers.delete(subscriber); await subscriber.unsubscribe(destination); await subscriber.quit(); } }); }
  async #consumeStream<TPayload>(destination: string, handler: QueueHandler<TPayload, unknown, RedisAcknowledgement>, options?: ConsumerOptions<RedisConsumerOptions>): Promise<QueueConsumer> { const client = await this.#getClient(); const group = this.config.group ?? 'queuekit'; const consumerName = this.config.consumer ?? `queuekit-${Math.random().toString(36).slice(2)}`; try { await client.sendCommand(['XGROUP', 'CREATE', destination, group, '0', 'MKSTREAM']); } catch { /* group can already exist */ }
    const controller = new AbortController(); let paused = false; const loop = async () => { while (!controller.signal.aborted) { if (paused) { await new Promise((resolve) => setTimeout(resolve, 20)); continue; } try { const response = await client.sendCommand<unknown>(['XREADGROUP', 'GROUP', group, consumerName, 'COUNT', String(options?.native?.count ?? 1), 'BLOCK', String(options?.native?.blockMs ?? 1000), 'STREAMS', destination, '>']); for (const item of streamEntries(response)) { const envelope = jsonCodec.decode(item.data) as RedisStreamMessage<TPayload>; const attempt = envelope.attempt ?? 1; const acknowledgement = createRedisStreamAcknowledgement(client, destination, group, item.id, envelope, attempt); const ack: RedisAcknowledgement = { ...acknowledgement, native: item.raw }; await handler({ message: { ...envelope, id: envelope.id ?? item.id, attempt, headers: envelope.headers ?? {}, native: item.raw }, ack, signal: controller.signal }); if (options?.autoAck) await ack.complete(); } } catch (error) { if (controller.signal.aborted) return; // A thrown handler or a dropped connection must not end the loop: the
      // consumer would keep the client's entries pending forever and say
      // nothing, which looks exactly like a queue that stopped working.
      console.error(`[queuekit/redis] consume loop error on ${destination}`, error); await new Promise((resolve) => setTimeout(resolve, 500)); } } }; void loop(); return createConsumer({ pause: async () => { paused = true; }, resume: async () => { paused = false; }, close: async () => { controller.abort(); } }); }
  native(): RedisClient | undefined { return this.#client; }
  async close(): Promise<void> { this.markClosed(); await Promise.all([...this.#subscribers].map((client) => client.quit())); await this.#client?.quit(); this.#subscribers.clear(); this.#client = undefined; }
}
/** One XREADGROUP entry whose `data` field carries the queuekit envelope. */
interface RedisStreamEntry { readonly id: string; readonly data: string; readonly raw: unknown }
/**
 * Normalizes the two shapes `sendCommand` returns for XREADGROUP across
 * node-redis versions: the raw array form `[[stream, [[id, ['data', json]]]]]`
 * (v4) and the parsed object form `{ stream: [[id, ['data', json]]] }` (v5+).
 * Only the array shape was understood, so on v5+ every read entry was
 * discarded — the message stayed pending and the handler never ran, with no
 * error anywhere.
 */
export function streamEntries(value: unknown): readonly RedisStreamEntry[] {
  if (value == null) return [];
  const streams: unknown[] = Array.isArray(value) ? value : Object.values(value as Record<string, unknown>);
  const entries: RedisStreamEntry[] = [];
  // Array form: each stream is [name, entries] — name is a string. Object
  // form: the values are the entries arrays themselves, so stream[0] is an
  // entry ([id, fields]) rather than a name. Testing the first element's type
  // is what tells the two apart; shape alone cannot, because an entries array
  // of two-plus entries has an array at index 1 too.
  for (const stream of streams) {
    const list = Array.isArray(stream) && typeof stream[0] === 'string' ? stream[1] : stream;
    if (!Array.isArray(list)) continue;
    for (const entry of list) { if (!Array.isArray(entry) || typeof entry[0] !== 'string' || !Array.isArray(entry[1])) continue; const index = entry[1].indexOf('data'); if (index >= 0 && typeof entry[1][index + 1] === 'string') entries.push({ id: entry[0], data: entry[1][index + 1] as string, raw: entry }); }
  }
  return entries;
}
export async function createRedisQueue(config: RedisConfig): Promise<RedisProvider> { return new RedisProvider(config); }
