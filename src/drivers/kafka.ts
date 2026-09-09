import type { ConsumerConfig, EachMessagePayload, ProducerRecord, RecordMetadata } from 'kafkajs';
import { BaseQueueProvider } from '../core/base-provider.js';
import { asText, jsonCodec } from '../core/codec.js';
import { createConsumer } from '../core/lifecycle.js';
import { loadOptional } from '../core/load-optional.js';
import type { KafkaConfig } from '../config.js';
import type { ConsumerOptions, PublishOptions, PublishResult, QueueAcknowledgement, QueueCapabilities, QueueConsumer, QueueHandler, QueueMessage } from '../core/types.js';

export type KafkaPublishOptions = Omit<ProducerRecord, 'topic' | 'messages'> & { readonly partition?: number; readonly headers?: Record<string, string | undefined> };
export type KafkaConsumerOptions = ConsumerConfig & { readonly fromBeginning?: boolean };
export type KafkaAcknowledgement = QueueAcknowledgement<EachMessagePayload>;

export class KafkaProvider extends BaseQueueProvider<'kafka'> {
  readonly name = 'kafka' as const;
  readonly capabilities: QueueCapabilities = { kind: 'stream', publish: true, consume: true, batchPublish: true, ack: true, partitions: true, consumerGroups: true, replay: true, ordering: true, transactions: true };
  #sdk: typeof import('kafkajs') | undefined; #producer: import('kafkajs').Producer | undefined; #kafka: import('kafkajs').Kafka | undefined; readonly #consumers = new Set<import('kafkajs').Consumer>();
  constructor(readonly config: KafkaConfig) { super(); }
  async #module(): Promise<typeof import('kafkajs')> { return this.#sdk ??= await loadOptional<typeof import('kafkajs')>('kafkajs', this.name); }
  async #getProducer(): Promise<import('kafkajs').Producer> { if (this.#producer) return this.#producer; const { Kafka } = await this.#module(); this.#kafka = new Kafka({ clientId: this.config.clientId, brokers: [...this.config.brokers], ssl: this.config.ssl, sasl: this.config.sasl as never, connectionTimeout: this.config.connectionTimeout, requestTimeout: this.config.requestTimeout }); this.#producer = this.#kafka.producer(); await this.#producer.connect(); return this.#producer; }
  async publish<TPayload>(destination: string, message: QueueMessage<TPayload>, options?: PublishOptions<KafkaPublishOptions>): Promise<PublishResult<'kafka', readonly RecordMetadata[]>> {
    let result: PublishResult<'kafka', readonly RecordMetadata[]> | undefined;
    await this.operation({ provider: this.name, operation: 'publish', destination, traceId: message.traceId, correlationId: message.correlationId, metadata: message.metadata }, async () => {
      const response = await (await this.#getProducer()).send({ ...options?.native, topic: destination, messages: [{ value: asText(jsonCodec.encode(message)), partition: options?.native?.partition, headers: { ...options?.native?.headers, ...stringHeaders(message) } }] });
      const first = response[0]; result = { ok: true, provider: this.name, messageId: message.id, partition: first?.partition, offset: first?.baseOffset, native: response };
    }); return result!;
  }
  async consume<TPayload>(destination: string, handler: QueueHandler<TPayload, EachMessagePayload, KafkaAcknowledgement>, options?: ConsumerOptions<KafkaConsumerOptions>): Promise<QueueConsumer> {
    const { Kafka } = await this.#module(); const kafka = this.#kafka ??= new Kafka({ clientId: this.config.clientId, brokers: [...this.config.brokers], ssl: this.config.ssl, sasl: this.config.sasl as never });
    const native = options?.native; if (!native?.groupId) throw new Error('Kafka consumers require native.groupId.');
    const consumer = kafka.consumer({ ...native }); await consumer.connect(); await consumer.subscribe({ topic: destination, fromBeginning: native.fromBeginning }); this.#consumers.add(consumer); const controller = new AbortController();
    await consumer.run({ autoCommit: false, eachMessage: async (raw) => { const encoded = raw.message.value; if (!encoded) return; const envelope = jsonCodec.decode(asText(encoded)) as QueueMessage<TPayload>; let completed = false; const ack: KafkaAcknowledgement = { native: raw, complete: async () => { if (!completed) { await consumer.commitOffsets([{ topic: raw.topic, partition: raw.partition, offset: (BigInt(raw.message.offset) + 1n).toString() }]); completed = true; } } }; await handler({ message: { id: envelope.id, type: envelope.type, payload: envelope.payload, headers: envelope.headers ?? {}, timestamp: envelope.timestamp, correlationId: envelope.correlationId, causationId: envelope.causationId, traceId: envelope.traceId, native: raw }, ack, signal: controller.signal }); if (options?.autoAck) await ack.complete(); } });
    return createConsumer({ pause: async () => consumer.pause([{ topic: destination }]), resume: async () => consumer.resume([{ topic: destination }]), close: async () => { controller.abort(); this.#consumers.delete(consumer); await consumer.disconnect(); } });
  }
  native(): { readonly kafka: import('kafkajs').Kafka | undefined; readonly producer: import('kafkajs').Producer | undefined } { return { kafka: this.#kafka, producer: this.#producer }; }
  async close(): Promise<void> { this.markClosed(); await Promise.all([...this.#consumers].map((consumer) => consumer.disconnect())); await this.#producer?.disconnect(); this.#consumers.clear(); this.#producer = undefined; }
}
function stringHeaders(message: QueueMessage): Record<string, string> { return Object.fromEntries(Object.entries({ 'x-queuekit-type': message.type, 'x-correlation-id': message.correlationId, 'x-causation-id': message.causationId, 'x-trace-id': message.traceId }).filter((entry): entry is [string, string] => typeof entry[1] === 'string')); }
export async function createKafka(config: KafkaConfig): Promise<KafkaProvider> { return new KafkaProvider(config); }
