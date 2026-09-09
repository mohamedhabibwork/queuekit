import type { JobsOptions, Job, WorkerOptions } from 'bullmq';
import { BaseQueueProvider } from '../core/base-provider.js';
import { createConsumer } from '../core/lifecycle.js';
import { loadOptional } from '../core/load-optional.js';
import type { BullMqConfig } from '../config.js';
import type { ConsumerOptions, PublishOptions, PublishResult, QueueAcknowledgement, QueueCapabilities, QueueConsumer, QueueHandler, QueueMessage } from '../core/types.js';

export type BullMqPublishOptions = JobsOptions;
export type BullMqConsumerOptions = Omit<WorkerOptions, 'connection' | 'prefix'>;
export type BullMqAcknowledgement = QueueAcknowledgement<Job<unknown>>;

export class BullMqProvider extends BaseQueueProvider<'bullmq'> {
  readonly name = 'bullmq' as const;
  readonly capabilities: QueueCapabilities = { kind: 'job', publish: true, consume: true, batchPublish: true, delayed: true, retries: true, priority: true, ttl: false, ack: true, scheduling: true };
  readonly #queues = new Map<string, InstanceType<typeof import('bullmq')['Queue']>>();
  readonly #workers = new Set<InstanceType<typeof import('bullmq')['Worker']>>();
  #sdk: typeof import('bullmq') | undefined;
  constructor(readonly config: BullMqConfig) { super(); }
  async #module(): Promise<typeof import('bullmq')> { return this.#sdk ??= await loadOptional<typeof import('bullmq')>('bullmq', this.name); }
  async #queue(destination: string): Promise<InstanceType<typeof import('bullmq')['Queue']>> {
    let queue = this.#queues.get(destination); if (queue) return queue;
    const { Queue } = await this.#module();
    queue = new Queue(destination, { connection: this.config.connection as never, prefix: this.config.prefix });
    this.#queues.set(destination, queue); return queue;
  }
  async publish<TPayload>(destination: string, message: QueueMessage<TPayload>, options?: PublishOptions<BullMqPublishOptions>): Promise<PublishResult<'bullmq', Job<QueueMessage<TPayload>>>> {
    let result: PublishResult<'bullmq', Job<QueueMessage<TPayload>>> | undefined;
    await this.operation({ provider: this.name, operation: 'publish', destination, traceId: message.traceId, correlationId: message.correlationId, metadata: message.metadata }, async () => {
      const job = await (await this.#queue(destination)).add(message.type ?? destination, message, { ...options?.native, delay: options?.delay ?? options?.native?.delay, priority: options?.priority ?? options?.native?.priority, jobId: options?.idempotencyKey ?? options?.native?.jobId });
      result = { ok: true, provider: this.name, messageId: job.id, native: job as Job<QueueMessage<TPayload>> };
    });
    return result!;
  }
  async publishMany<TPayload>(destination: string, messages: readonly QueueMessage<TPayload>[], options?: PublishOptions<BullMqPublishOptions>): Promise<readonly PublishResult<'bullmq', Job<QueueMessage<TPayload>>>[]> {
    return Promise.all(messages.map((message) => this.publish(destination, message, options)));
  }
  async consume<TPayload>(destination: string, handler: QueueHandler<TPayload, Job<QueueMessage<TPayload>>, BullMqAcknowledgement>, options?: ConsumerOptions<BullMqConsumerOptions>): Promise<QueueConsumer> {
    const { Worker } = await this.#module(); const controller = new AbortController();
    const worker = new Worker(destination, async (job) => {
      const envelope = job.data as QueueMessage<TPayload>;
      const ack: BullMqAcknowledgement = { native: job as Job<unknown>, complete: async () => undefined };
      await handler({ message: { id: job.id, type: envelope.type, payload: envelope.payload, headers: envelope.headers ?? {}, timestamp: envelope.timestamp, correlationId: envelope.correlationId, causationId: envelope.causationId, traceId: envelope.traceId, native: job }, ack, signal: controller.signal });
    }, { connection: this.config.connection as never, prefix: this.config.prefix, concurrency: options?.concurrency, ...options?.native });
    this.#workers.add(worker);
    return createConsumer({ pause: async () => worker.pause(), resume: async () => worker.resume(), close: async () => { controller.abort(); this.#workers.delete(worker); await worker.close(); } });
  }
  native(): { readonly queues: ReadonlyMap<string, InstanceType<typeof import('bullmq')['Queue']>>; readonly workers: ReadonlySet<InstanceType<typeof import('bullmq')['Worker']>> } { return { queues: this.#queues, workers: this.#workers }; }
  async close(): Promise<void> { this.markClosed(); await Promise.all([...this.#workers].map((worker) => worker.close())); await Promise.all([...this.#queues.values()].map((queue) => queue.close())); this.#workers.clear(); this.#queues.clear(); }
}
export async function createBullMQ(config: BullMqConfig): Promise<BullMqProvider> { return new BullMqProvider(config); }
