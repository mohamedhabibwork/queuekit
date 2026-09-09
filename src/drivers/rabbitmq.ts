import type { ConsumeMessage, Options } from 'amqplib';
import { BaseQueueProvider } from '../core/base-provider.js';
import { asText, jsonCodec } from '../core/codec.js';
import { createConsumer } from '../core/lifecycle.js';
import { loadOptional } from '../core/load-optional.js';
import type { RabbitMqConfig } from '../config.js';
import type { ConsumerOptions, PublishOptions, PublishResult, QueueAcknowledgement, QueueCapabilities, QueueConsumer, QueueHandler, QueueMessage } from '../core/types.js';

export type RabbitMqPublishOptions = Options.Publish;
export type RabbitMqConsumerOptions = Options.Consume & { readonly prefetch?: number };
export type RabbitMqAcknowledgement = QueueAcknowledgement<ConsumeMessage>;
export class RabbitMqProvider extends BaseQueueProvider<'rabbitmq'> {
  readonly name = 'rabbitmq' as const;
  readonly capabilities: QueueCapabilities = { kind: 'queue', publish: true, consume: true, delayed: false, retries: true, priority: true, ttl: true, deadLetter: true, ack: true, nack: true, ordering: true };
  #sdk: typeof import('amqplib') | undefined; #connection: import('amqplib').ChannelModel | undefined; #channel: import('amqplib').Channel | undefined; readonly #consumers = new Set<string>();
  constructor(readonly config: RabbitMqConfig) { super(); }
  async #module(): Promise<typeof import('amqplib')> { return this.#sdk ??= await loadOptional<typeof import('amqplib')>('amqplib', this.name); }
  async #getChannel(): Promise<import('amqplib').Channel> { if (this.#channel) return this.#channel; const sdk = await this.#module(); this.#connection = await sdk.connect(toUrl(this.config.url), this.config.heartbeat ? { heartbeat: this.config.heartbeat } : undefined); this.#channel = await this.#connection.createChannel(); return this.#channel; }
  async publish<TPayload>(destination: string, message: QueueMessage<TPayload>, options?: PublishOptions<RabbitMqPublishOptions>): Promise<PublishResult<'rabbitmq', boolean>> {
    let result: PublishResult<'rabbitmq', boolean> | undefined;
    await this.operation({ provider: this.name, operation: 'publish', destination, traceId: message.traceId, correlationId: message.correlationId, metadata: message.metadata }, async () => { const channel = await this.#getChannel(); await channel.assertQueue(destination); const native = channel.sendToQueue(destination, Buffer.from(asText(jsonCodec.encode(message))), { ...options?.native, expiration: options?.ttl?.toString() ?? options?.native?.expiration, priority: options?.priority ?? options?.native?.priority, messageId: message.id ?? options?.native?.messageId, correlationId: message.correlationId ?? options?.native?.correlationId, headers: { ...options?.native?.headers, 'x-queuekit-type': message.type, 'x-causation-id': message.causationId, 'x-trace-id': message.traceId } }); result = { ok: native, provider: this.name, messageId: message.id, native }; }); return result!;
  }
  async consume<TPayload>(destination: string, handler: QueueHandler<TPayload, ConsumeMessage, RabbitMqAcknowledgement>, options?: ConsumerOptions<RabbitMqConsumerOptions>): Promise<QueueConsumer> {
    const channel = await this.#getChannel(); await channel.assertQueue(destination); if (options?.native?.prefetch ?? options?.concurrency) await channel.prefetch(options?.native?.prefetch ?? options?.concurrency ?? 1);
    const controller = new AbortController(); let consumerTag = '';
    const start = async () => { const response = await channel.consume(destination, async (raw) => { if (!raw) return; const envelope = jsonCodec.decode(raw.content) as QueueMessage<TPayload>; let settled = false; const ack: RabbitMqAcknowledgement = { native: raw, complete: async () => { if (!settled) { channel.ack(raw); settled = true; } }, retry: async () => { if (!settled) { channel.nack(raw, false, true); settled = true; } }, reject: async (reject) => { if (!settled) { channel.reject(raw, reject?.requeue ?? false); settled = true; } } }; try { await handler({ message: { id: envelope.id ?? raw.properties.messageId, type: envelope.type, payload: envelope.payload, headers: envelope.headers ?? {}, timestamp: envelope.timestamp ?? raw.properties.timestamp, correlationId: envelope.correlationId ?? raw.properties.correlationId, causationId: envelope.causationId, traceId: envelope.traceId, native: raw }, ack, signal: controller.signal }); if (options?.autoAck) await ack.complete(); } catch (error) { if (!settled) channel.nack(raw, false, true); throw error; } }, { noAck: false, ...options?.native }); consumerTag = response.consumerTag; this.#consumers.add(consumerTag); };
    await start(); return createConsumer({ pause: async () => { await channel.cancel(consumerTag); }, resume: async () => { await start(); }, close: async () => { controller.abort(); this.#consumers.delete(consumerTag); if (consumerTag) await channel.cancel(consumerTag); } });
  }
  native(): { readonly connection: import('amqplib').ChannelModel | undefined; readonly channel: import('amqplib').Channel | undefined } { return { connection: this.#connection, channel: this.#channel }; }
  async close(): Promise<void> { this.markClosed(); await Promise.all([...this.#consumers].map((tag) => this.#channel?.cancel(tag))); await this.#channel?.close(); await this.#connection?.close(); this.#consumers.clear(); this.#channel = undefined; this.#connection = undefined; }
}
function toUrl(value: RabbitMqConfig['url']): string { if (typeof value === 'string') return value; const auth = value.username ? `${encodeURIComponent(value.username)}:${encodeURIComponent(value.password ?? '')}@` : ''; return `amqp://${auth}${value.hostname}:${value.port ?? 5672}/${encodeURIComponent(value.vhost ?? '')}`; }
export async function createRabbitMQ(config: RabbitMqConfig): Promise<RabbitMqProvider> { return new RabbitMqProvider(config); }
