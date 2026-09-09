import type { Msg, NatsConnection, PublishOptions as NatsPublishOptions } from 'nats';
import { BaseQueueProvider } from '../core/base-provider.js';
import { asBytes, jsonCodec } from '../core/codec.js';
import { createConsumer } from '../core/lifecycle.js';
import { loadOptional } from '../core/load-optional.js';
import type { NatsConfig } from '../config.js';
import type { ConsumerOptions, PublishOptions, PublishResult, QueueAcknowledgement, QueueCapabilities, QueueConsumer, QueueHandler, QueueMessage } from '../core/types.js';

export type NatsPublishNativeOptions = NatsPublishOptions;
export interface NatsConsumerOptions { readonly queue?: string; }
export type NatsAcknowledgement = QueueAcknowledgement<Msg>;
export class NatsProvider extends BaseQueueProvider<'nats'> {
  readonly name = 'nats' as const;
  readonly capabilities: QueueCapabilities;
  #sdk: typeof import('nats') | undefined; #connection: NatsConnection | undefined;
  constructor(readonly config: NatsConfig) { super(); this.capabilities = { kind: config.mode === 'jetstream' ? 'stream' : 'pubsub', publish: true, consume: config.mode !== 'jetstream', ack: config.mode === 'jetstream', replay: config.mode === 'jetstream', consumerGroups: config.mode === 'jetstream', ordering: config.mode === 'jetstream', requestReply: config.mode !== 'jetstream' }; }
  async #module(): Promise<typeof import('nats')> { return this.#sdk ??= await loadOptional<typeof import('nats')>('nats', this.name); }
  async #getConnection(): Promise<NatsConnection> { if (this.#connection) return this.#connection; this.#connection = await (await this.#module()).connect({ servers: typeof this.config.servers === 'string' ? this.config.servers : [...this.config.servers], name: this.config.name, token: this.config.token, user: this.config.user, pass: this.config.pass }); return this.#connection; }
  async publish<TPayload>(destination: string, message: QueueMessage<TPayload>, options?: PublishOptions<NatsPublishNativeOptions>): Promise<PublishResult<'nats', void | import('nats').PubAck>> {
    let result: PublishResult<'nats', void | import('nats').PubAck> | undefined; await this.operation({ provider: this.name, operation: 'publish', destination, traceId: message.traceId, correlationId: message.correlationId, metadata: message.metadata }, async () => { const connection = await this.#getConnection(); const bytes = asBytes(jsonCodec.encode(message)); const native = this.config.mode === 'jetstream' ? await connection.jetstream().publish(destination, bytes, options?.native) : connection.publish(destination, bytes, options?.native); result = { ok: true, provider: this.name, messageId: message.id, sequenceNumber: this.config.mode === 'jetstream' ? String((native as import('nats').PubAck).seq) : undefined, native }; }); return result!;
  }
  async consume<TPayload>(destination: string, handler: QueueHandler<TPayload, Msg, NatsAcknowledgement>, options?: ConsumerOptions<NatsConsumerOptions>): Promise<QueueConsumer> { if (this.config.mode === 'jetstream') throw new Error('JetStream pull-consumer configuration is provider-native and is not exposed by the v0.1 generic consumer. Use native().jetstream() for it.'); const subscription = (await this.#getConnection()).subscribe(destination, { queue: options?.native?.queue }); const controller = new AbortController(); let paused = false; const run = async () => { for await (const raw of subscription) { if (controller.signal.aborted) break; while (paused && !controller.signal.aborted) await new Promise((resolve) => setTimeout(resolve, 20)); const envelope = jsonCodec.decode(raw.data) as QueueMessage<TPayload>; const ack: NatsAcknowledgement = { native: raw, complete: async () => undefined }; await handler({ message: { ...envelope, headers: envelope.headers ?? {}, native: raw }, ack, signal: controller.signal }); } }; void run(); return createConsumer({ pause: async () => { paused = true; }, resume: async () => { paused = false; }, close: async () => { controller.abort(); subscription.unsubscribe(); } }); }
  native(): NatsConnection | undefined { return this.#connection; }
  async close(): Promise<void> { this.markClosed(); await this.#connection?.drain(); this.#connection = undefined; }
}
export async function createNats(config: NatsConfig): Promise<NatsProvider> { return new NatsProvider(config); }
