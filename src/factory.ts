import { QueueConfigError } from './core/errors.js';
import type { BuiltInQueueConfig, BullMqConfig, KafkaConfig, MemoryConfig, NatsConfig, RabbitMqConfig, RedisConfig, SqsConfig } from './config.js';
import type { QueueProvider } from './core/types.js';
import type { BullMqProvider } from './drivers/bullmq.js';
import type { KafkaProvider } from './drivers/kafka.js';
import type { MemoryQueue } from './testing/memory-queue.js';
import type { NatsProvider } from './drivers/nats.js';
import type { RabbitMqProvider } from './drivers/rabbitmq.js';
import type { RedisProvider } from './drivers/redis.js';
import type { SqsProvider } from './drivers/sqs.js';
export type QueueForConfig<TConfig> = TConfig extends MemoryConfig ? MemoryQueue : TConfig extends BullMqConfig ? BullMqProvider : TConfig extends KafkaConfig ? KafkaProvider : TConfig extends RabbitMqConfig ? RabbitMqProvider : TConfig extends RedisConfig ? RedisProvider : TConfig extends NatsConfig ? NatsProvider : TConfig extends SqsConfig ? SqsProvider : never;
export async function createQueue<const TConfig extends BuiltInQueueConfig>(config: TConfig): Promise<QueueForConfig<TConfig>> { switch (config.type) { case 'memory': return Promise.resolve((await import('./testing/memory-queue.js')).createMemoryQueue(config)) as Promise<QueueForConfig<TConfig>>; case 'bullmq': return (await import('./drivers/bullmq.js')).createBullMQ(config) as Promise<QueueForConfig<TConfig>>; case 'kafka': return (await import('./drivers/kafka.js')).createKafka(config) as Promise<QueueForConfig<TConfig>>; case 'rabbitmq': return (await import('./drivers/rabbitmq.js')).createRabbitMQ(config) as Promise<QueueForConfig<TConfig>>; case 'redis': return (await import('./drivers/redis.js')).createRedisQueue(config) as Promise<QueueForConfig<TConfig>>; case 'nats': return (await import('./drivers/nats.js')).createNats(config) as Promise<QueueForConfig<TConfig>>; case 'sqs': return (await import('./drivers/sqs.js')).createSqs(config) as Promise<QueueForConfig<TConfig>>; default: throw new QueueConfigError(`Unsupported queue provider: ${(config as { type: string }).type}.`); } }
export type { BuiltInQueueConfig } from './config.js';
