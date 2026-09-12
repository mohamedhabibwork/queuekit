export interface MemoryConfig { readonly type: 'memory'; readonly latency?: number }
export interface BullMqConfig { readonly type: 'bullmq'; readonly connection: string | { readonly host: string; readonly port: number; readonly username?: string; readonly password?: string; readonly db?: number }; readonly prefix?: string; readonly queue?: string; }
export interface KafkaConfig { readonly type: 'kafka'; readonly clientId: string; readonly brokers: readonly string[]; readonly ssl?: boolean | object; readonly sasl?: unknown; readonly connectionTimeout?: number; readonly requestTimeout?: number; }
export interface RabbitMqConfig { readonly type: 'rabbitmq'; readonly url: string | { readonly hostname: string; readonly port?: number; readonly username?: string; readonly password?: string; readonly vhost?: string }; readonly heartbeat?: number; }
export interface RedisConfig { readonly type: 'redis'; readonly mode: 'pubsub' | 'streams'; readonly url: string; readonly group?: string; readonly consumer?: string; }
export interface NatsConfig { readonly type: 'nats'; readonly servers: string | readonly string[]; readonly name?: string; readonly token?: string; readonly user?: string; readonly pass?: string; readonly mode?: 'core' | 'jetstream'; }
export interface SqsConfig { readonly type: 'sqs'; readonly region: string; readonly queueUrl?: string; readonly endpoint?: string; readonly credentials?: unknown; }
export type BuiltInQueueConfig = MemoryConfig | BullMqConfig | KafkaConfig | RabbitMqConfig | RedisConfig | NatsConfig | SqsConfig;
