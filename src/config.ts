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
/**
 * A Cloudflare Workers Queues binding (`env.MY_QUEUE`). When set, the provider
 * produces through the binding (in-Worker, no API token needed); consumers
 * always use the HTTP pull API.
 */
export interface CloudflareQueueBinding {
  readonly send: (
    message: unknown,
    options?: { readonly contentType?: string; readonly delaySeconds?: number },
  ) => Promise<unknown>;
  readonly sendBatch?: (
    messages: readonly unknown[],
    options?: { readonly contentType?: string; readonly delaySeconds?: number },
  ) => Promise<unknown>;
}
export interface CloudflareConfig {
  readonly type: "cloudflare";
  /** Queue ID (UUID) — `GET /accounts/{account_id}/queues` or the dashboard. */
  readonly queueId: string;
  /** Account ID. Required unless a `binding` is set (Workers publish path). */
  readonly accountId?: string;
  /** API token with Queues read + write (pull consumers acknowledge, so they need both). */
  readonly apiToken?: string;
  /** Workers Queues binding — publish-only, replaces the HTTP push API when set. */
  readonly binding?: CloudflareQueueBinding;
  /** Override for tests and API gateways. @default "https://api.cloudflare.com/client/v4" */
  readonly apiBaseUrl?: string;
  /** Pull lease time in ms (Cloudflare default 30s, max 12h). */
  readonly visibilityTimeoutMs?: number;
  /** Pull batch size (Cloudflare default 5, max 100). */
  readonly batchSize?: number;
}
export interface GooglePubSubConfig {
  readonly type: "gcpubsub";
  readonly projectId: string;
  /** Topic name or fully-qualified path to publish to. */
  readonly topic: string;
  /** Subscription name or path for `consume()` — required to consume. */
  readonly subscription?: string;
  /** Override API endpoint, e.g. the Pub/Sub emulator (`http://localhost:8085`). */
  readonly endpoint?: string;
  readonly credentials?: unknown;
  readonly keyFilename?: string;
}
export interface AzureServiceBusConfig {
  readonly type: "azureservicebus";
  /** Connection string, or a `fullyQualifiedNamespace` + `credential` pair. */
  readonly connectionString?: string;
  readonly fullyQualifiedNamespace?: string;
  readonly credential?: unknown;
  /** Default queue (or topic name) used when `destination` is omitted. */
  readonly queue?: string;
  /** @default "peekLock" — "receiveAndDelete" settles messages on delivery. */
  readonly receiveMode?: "peekLock" | "receiveAndDelete";
}
export type BuiltInQueueConfig =
  | MemoryConfig
  | BullMqConfig
  | KafkaConfig
  | RabbitMqConfig
  | RedisConfig
  | NatsConfig
  | SqsConfig
  | CloudflareConfig
  | GooglePubSubConfig
  | AzureServiceBusConfig;
