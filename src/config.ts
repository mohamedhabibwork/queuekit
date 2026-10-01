export interface MemoryConfig {
  readonly type: "memory";
  readonly latency?: number;
}
export interface BullMqConfig {
  readonly type: "bullmq";
  readonly connection:
    | string
    | {
        readonly host: string;
        readonly port: number;
        readonly username?: string;
        readonly password?: string;
        readonly db?: number;
      };
  readonly prefix?: string;
  readonly queue?: string;
}
export interface KafkaConfig {
  readonly type: "kafka";
  readonly clientId: string;
  readonly brokers: readonly string[];
  readonly ssl?: boolean | object;
  readonly sasl?: unknown;
  readonly connectionTimeout?: number;
  readonly requestTimeout?: number;
}
export interface RabbitMqConfig {
  readonly type: "rabbitmq";
  readonly url:
    | string
    | {
        readonly hostname: string;
        readonly port?: number;
        readonly username?: string;
        readonly password?: string;
        readonly vhost?: string;
        /** Use `amqps://` (TLS); port defaults to 5671 when unset. */
        readonly tls?: boolean;
      };
  readonly heartbeat?: number;
}
export interface RedisConfig {
  readonly type: "redis";
  readonly mode: "pubsub" | "streams";
  readonly url: string;
  readonly group?: string;
  readonly consumer?: string;
  /** Which optional peer SDK to drive the connection with. Both work against Redis and Valkey servers. @default 'redis' */ readonly client?:
    | "redis"
    | "ioredis";
}
export interface NatsConfig {
  readonly type: "nats";
  readonly servers: string | readonly string[];
  readonly name?: string;
  readonly token?: string;
  readonly user?: string;
  readonly pass?: string;
  /** Passed straight to nats.js `connect` (e.g. `{ ca: [...] }`); `tls: true` requires TLS. */
  readonly tls?: boolean | object;
  readonly mode?: "core" | "jetstream";
}
export interface SqsConfig {
  readonly type: "sqs";
  readonly region: string;
  readonly queueUrl?: string;
  readonly endpoint?: string;
  readonly credentials?: unknown;
}
export type BuiltInQueueConfig =
  | MemoryConfig
  | BullMqConfig
  | KafkaConfig
  | RabbitMqConfig
  | RedisConfig
  | NatsConfig
  | SqsConfig;
