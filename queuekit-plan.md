# QueueKit — Architecture & Implementation Plan

> Unified TypeScript queue and messaging infrastructure for Node.js, Bun, and Deno — while preserving native provider semantics, configuration, and strongly typed options.

Proposed package:

```text
@mohamedhabibwork/queuekit
```

This document assumes:

```text
@mohamedhabibwork/queuekit
```

---

# 1. Vision

QueueKit should provide one consistent TypeScript architecture for working with multiple message queue, job queue, pub/sub, and streaming providers without flattening them into a lowest-common-denominator API.

The package should support:

```text
Kafka
RabbitMQ
BullMQ
Redis
NATS
Amazon SQS
Amazon SNS
Google Cloud Pub/Sub
Azure Service Bus
custom providers
```

with future support for:

```text
Apache Pulsar
ActiveMQ / Artemis
NSQ
Redpanda
Valkey
Dragonfly
Upstash
Cloudflare Queues
IronMQ
MQTT
ZeroMQ
custom HTTP queues
internal queue services
```

The design should be inspired by:

```text
@mohamedhabibwork/storagekit
@mohamedhabibwork/notificationkit
```

Key promise:

```text
Unified TypeScript queue infrastructure across multiple providers —
with strongly typed native provider options preserved instead of
flattened into a lowest-common-denominator API.
```

---

# 2. Core Goals

QueueKit should:

- support Node.js
- support Bun
- support Deno
- support TypeScript 5.9
- support TypeScript 6.x
- support TypeScript 7.x
- be ESM-first
- remain tree-shakeable
- use optional peer dependencies
- provide provider-specific entrypoints
- preserve native SDK types
- support custom providers
- provide strongly typed payloads
- provide strongly typed queue names
- provide strongly typed topics
- provide strongly typed event payloads
- provide job queues
- provide message queues
- provide pub/sub
- provide streams
- provide delayed jobs where supported
- provide retries
- provide dead-letter handling abstractions
- provide acknowledgement semantics
- provide visibility timeout / lock concepts without pretending all providers behave identically
- provide provider-native escape hatches
- provide middleware
- provide observability hooks
- provide testing/fake providers
- provide lifecycle management
- provide producer and consumer abstractions
- support multiple named provider connections
- support graceful shutdown
- avoid forcing Redis concepts onto Kafka
- avoid forcing Kafka concepts onto BullMQ
- avoid hiding RabbitMQ exchange/routing semantics
- avoid hiding provider-native configuration

---

# 3. Non-Goals

QueueKit should not become:

```text
a distributed workflow engine
a Temporal replacement
a Kafka Streams replacement
a full event sourcing framework
a database
a schema registry server
a scheduler service
a monitoring dashboard
a queue broker
```

QueueKit should connect applications to brokers and queue systems.

It should not try to implement a broker itself.

---

# 4. Runtime Support

Recommended runtime matrix:

| Runtime | Minimum | Support |
|---|---:|---|
| Node.js | `>=20` | First-class |
| Bun | `>=1.1` | First-class |
| Deno | `>=2.0` | First-class |
| Browser | N/A | Not supported |

Some provider SDKs may have runtime-specific limitations.

QueueKit core must remain runtime-neutral.

Provider drivers may document runtime restrictions where upstream libraries require them.

---

# 5. TypeScript Support

Supported range:

```text
>=5.9 <8
```

CI should test:

```text
TypeScript 5.9
TypeScript 6.x
TypeScript 7.x
```

Avoid public declaration syntax that prevents consumption by TypeScript 5.9.

Avoid dependence on unstable compiler internals.

Types are part of the product.

---

# 6. Main Architecture Principle

Do not expose only:

```ts
queue.send(...)
queue.receive(...)
```

and pretend every provider behaves the same.

Instead expose a shared foundation plus provider capabilities.

Example:

```ts
const queue = await createQueue({
  type: 'bullmq',

  connection: {
    host: 'localhost',
    port: 6379,
  },

  queue: 'emails',
});

await queue.publish(
  {
    type: 'welcome-email',
    payload: {
      userId: '123',
      email: 'user@example.com',
    },
  },
  {
    native: {
      delay: 5000,
      attempts: 5,
      backoff: {
        type: 'exponential',
        delay: 1000,
      },
    },
  },
);
```

For Kafka:

```ts
const kafka = await createQueue({
  type: 'kafka',

  brokers: [
    'localhost:9092',
  ],

  clientId: 'billing-service',
});

await kafka.publish(
  'payments.completed',
  {
    paymentId: 'pay_123',
    amount: 100,
  },
  {
    native: {
      partition: 2,
      headers: {
        traceId: 'abc',
      },
    },
  },
);
```

The `native` type must change based on provider.

---

# 7. Queue Families

QueueKit should explicitly model four major messaging families.

## Job Queue

Examples:

```text
BullMQ
Redis-backed custom queues
SQS-based worker queues
```

Typical semantics:

```text
jobs
attempts
delay
backoff
priority
worker concurrency
completion/failure
```

---

## Message Queue

Examples:

```text
RabbitMQ
SQS
Azure Service Bus queues
```

Typical semantics:

```text
ack
nack
visibility timeout
routing
dead-letter queues
consumer groups
```

---

## Pub/Sub

Examples:

```text
Redis Pub/Sub
Google Pub/Sub
SNS
NATS
RabbitMQ exchanges
```

Typical semantics:

```text
topic
subscriber
fanout
broadcast
subscriptions
```

---

## Event Stream

Examples:

```text
Kafka
Redpanda
Redis Streams
Pulsar
NATS JetStream
```

Typical semantics:

```text
partition
offset
consumer group
retention
replay
ordered streams
```

QueueKit must not hide these distinctions.

---

# 8. Proposed Repository Structure

```text
queuekit/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       ├── runtimes.yml
│       ├── typescript.yml
│       └── release.yml
│
├── docs/
│   ├── architecture.md
│   ├── custom-providers.md
│   ├── providers.md
│   ├── job-queues.md
│   ├── message-queues.md
│   ├── pubsub.md
│   ├── streams.md
│   ├── retries.md
│   ├── dead-letter.md
│   ├── errors.md
│   ├── observability.md
│   ├── kafka.md
│   ├── rabbitmq.md
│   ├── bullmq.md
│   ├── redis.md
│   ├── nats.md
│   ├── sqs.md
│   ├── google-pubsub.md
│   └── azure-service-bus.md
│
├── src/
│   ├── core/
│   │   ├── message.ts
│   │   ├── job.ts
│   │   ├── producer.ts
│   │   ├── consumer.ts
│   │   ├── provider.ts
│   │   ├── capabilities.ts
│   │   ├── errors.ts
│   │   ├── result.ts
│   │   ├── acknowledgement.ts
│   │   ├── middleware.ts
│   │   ├── lifecycle.ts
│   │   ├── serialization.ts
│   │   └── utils.ts
│   │
│   ├── drivers/
│   │   ├── kafka/
│   │   │   ├── config.ts
│   │   │   ├── driver.ts
│   │   │   ├── producer.ts
│   │   │   ├── consumer.ts
│   │   │   ├── types.ts
│   │   │   └── index.ts
│   │   │
│   │   ├── rabbitmq/
│   │   │   ├── config.ts
│   │   │   ├── connection.ts
│   │   │   ├── producer.ts
│   │   │   ├── consumer.ts
│   │   │   ├── types.ts
│   │   │   └── index.ts
│   │   │
│   │   ├── bullmq/
│   │   │   ├── config.ts
│   │   │   ├── queue.ts
│   │   │   ├── worker.ts
│   │   │   ├── scheduler.ts
│   │   │   ├── types.ts
│   │   │   └── index.ts
│   │   │
│   │   ├── redis/
│   │   │   ├── config.ts
│   │   │   ├── pubsub.ts
│   │   │   ├── streams.ts
│   │   │   ├── list-queue.ts
│   │   │   ├── types.ts
│   │   │   └── index.ts
│   │   │
│   │   ├── nats/
│   │   │   ├── config.ts
│   │   │   ├── core.ts
│   │   │   ├── jetstream.ts
│   │   │   ├── types.ts
│   │   │   └── index.ts
│   │   │
│   │   ├── sqs/
│   │   │   ├── config.ts
│   │   │   ├── producer.ts
│   │   │   ├── consumer.ts
│   │   │   ├── types.ts
│   │   │   └── index.ts
│   │   │
│   │   ├── google-pubsub/
│   │   │   ├── config.ts
│   │   │   ├── producer.ts
│   │   │   ├── consumer.ts
│   │   │   ├── types.ts
│   │   │   └── index.ts
│   │   │
│   │   └── azure-service-bus/
│   │       ├── config.ts
│   │       ├── producer.ts
│   │       ├── consumer.ts
│   │       ├── types.ts
│   │       └── index.ts
│   │
│   ├── adapters/
│   │   ├── serializer.ts
│   │   ├── codec.ts
│   │   └── provider-response.ts
│   │
│   ├── registry/
│   │   ├── queue-registry.ts
│   │   ├── topic-registry.ts
│   │   └── schema-registry.ts
│   │
│   ├── manager/
│   │   ├── manager.ts
│   │   ├── connection.ts
│   │   └── aliases.ts
│   │
│   ├── testing/
│   │   ├── fake-provider.ts
│   │   ├── memory-queue.ts
│   │   ├── producer-spy.ts
│   │   ├── consumer-spy.ts
│   │   └── assertions.ts
│   │
│   ├── custom/
│   │   ├── define-provider.ts
│   │   └── types.ts
│   │
│   ├── factory.ts
│   ├── manager.ts
│   └── index.ts
│
├── tests/
│   ├── core/
│   ├── providers/
│   ├── contracts/
│   ├── typing/
│   └── runtimes/
│
├── package.json
├── tsconfig.json
├── tsconfig.build.json
├── tsup.config.ts
└── README.md
```

---

# 9. Public Entrypoints

Recommended imports:

```ts
import {
  createQueue,
  createQueueManager,
} from '@mohamedhabibwork/queuekit';
```

Provider-specific:

```ts
import {
  createKafka,
  KafkaProvider,
} from '@mohamedhabibwork/queuekit/kafka';

import {
  createRabbitMQ,
} from '@mohamedhabibwork/queuekit/rabbitmq';

import {
  createBullMQ,
} from '@mohamedhabibwork/queuekit/bullmq';

import {
  createRedisQueue,
} from '@mohamedhabibwork/queuekit/redis';

import {
  createNats,
} from '@mohamedhabibwork/queuekit/nats';

import {
  createSqs,
} from '@mohamedhabibwork/queuekit/sqs';

import {
  defineQueueProvider,
} from '@mohamedhabibwork/queuekit/custom';

import {
  createMemoryQueue,
} from '@mohamedhabibwork/queuekit/testing';
```

Recommended exports:

```json
{
  ".": {
    "types": "./dist/index.d.ts",
    "import": "./dist/index.js"
  },
  "./kafka": {
    "types": "./dist/kafka.d.ts",
    "import": "./dist/kafka.js"
  },
  "./rabbitmq": {
    "types": "./dist/rabbitmq.d.ts",
    "import": "./dist/rabbitmq.js"
  },
  "./bullmq": {
    "types": "./dist/bullmq.d.ts",
    "import": "./dist/bullmq.js"
  },
  "./redis": {
    "types": "./dist/redis.d.ts",
    "import": "./dist/redis.js"
  },
  "./nats": {
    "types": "./dist/nats.d.ts",
    "import": "./dist/nats.js"
  },
  "./sqs": {
    "types": "./dist/sqs.d.ts",
    "import": "./dist/sqs.js"
  },
  "./google-pubsub": {
    "types": "./dist/google-pubsub.d.ts",
    "import": "./dist/google-pubsub.js"
  },
  "./azure-service-bus": {
    "types": "./dist/azure-service-bus.d.ts",
    "import": "./dist/azure-service-bus.js"
  },
  "./custom": {
    "types": "./dist/custom.d.ts",
    "import": "./dist/custom.js"
  },
  "./testing": {
    "types": "./dist/testing.d.ts",
    "import": "./dist/testing.js"
  }
}
```

---

# 10. Base Message Model

Keep the normalized message intentionally small.

```ts
export interface QueueMessage<
  TPayload = unknown,
  THeaders extends Record<string, unknown> = Record<string, unknown>,
> {
  id?: string;

  type?: string;

  payload: TPayload;

  headers?: THeaders;

  correlationId?: string;

  causationId?: string;

  traceId?: string;

  timestamp?: number;

  metadata?: Record<string, unknown>;
}
```

Provider-native details should not be added here.

---

# 11. Publish Options

Use generics for native options.

```ts
export interface PublishOptions<
  TNative = unknown,
> {
  delay?: number;

  priority?: number;

  ttl?: number;

  idempotencyKey?: string;

  native?: TNative;

  signal?: AbortSignal;
}
```

Important:

```text
delay
priority
ttl
```

are only normalized convenience fields.

The provider must expose capability information indicating whether they are supported.

---

# 12. Publish Result

```ts
export interface PublishResult<
  TProvider extends string = string,
  TNative = unknown,
> {
  ok: boolean;

  provider: TProvider;

  messageId?: string;

  partition?: number;

  offset?: string;

  sequenceNumber?: string;

  native: TNative;
}
```

Do not fake values that providers do not return.

---

# 13. Consumer Message

```ts
export interface ConsumedMessage<
  TPayload,
  TNativeMessage = unknown,
> {
  id?: string;

  payload: TPayload;

  headers: Record<string, unknown>;

  attempt?: number;

  timestamp?: number;

  native: TNativeMessage;
}
```

Acknowledgement should be separate:

```ts
export interface MessageContext<
  TPayload,
  TNativeMessage,
  TAck,
> {
  message: ConsumedMessage<
    TPayload,
    TNativeMessage
  >;

  ack: TAck;

  signal: AbortSignal;
}
```

---

# 14. Acknowledgement Contract

Providers handle acknowledgement differently.

Base API:

```ts
export interface QueueAcknowledgement {
  complete(): Promise<void>;

  retry?(
    options?: RetryOptions,
  ): Promise<void>;

  reject?(
    options?: RejectOptions,
  ): Promise<void>;
}
```

Provider-specific acknowledgement is also exposed:

```ts
ack.native
```

For example:

```text
RabbitMQ:
ack / nack / reject

Kafka:
commit offsets

SQS:
delete message / change visibility

BullMQ:
return / throw / move job

Google Pub/Sub:
ack / nack / modify ack deadline
```

The normalized API should map only valid concepts.

---

# 15. Provider Capabilities

```ts
export interface QueueCapabilities {
  kind:
    | 'job'
    | 'queue'
    | 'pubsub'
    | 'stream'
    | 'hybrid';

  publish: boolean;

  consume: boolean;

  batchPublish?: boolean;

  batchConsume?: boolean;

  delayed?: boolean;

  retries?: boolean;

  priority?: boolean;

  ttl?: boolean;

  deadLetter?: boolean;

  ack?: boolean;

  nack?: boolean;

  replay?: boolean;

  partitions?: boolean;

  consumerGroups?: boolean;

  transactions?: boolean;

  ordering?: boolean;

  scheduling?: boolean;
}
```

Usage:

```ts
if (queue.capabilities.delayed) {
  await queue.publish(message, {
    delay: 5000,
  });
}
```

---

# 16. Generic Provider Contract

```ts
export interface QueueProvider<
  TName extends string,
  TConfig,
  TPublishNative,
  TPublishResponse,
  TConsumeNative,
  TAcknowledgement,
> {
  readonly name: TName;

  readonly capabilities: QueueCapabilities;

  publish<TPayload>(
    destination: string,
    message: QueueMessage<TPayload>,
    options?: PublishOptions<TPublishNative>,
  ): Promise<
    PublishResult<
      TName,
      TPublishResponse
    >
  >;

  consume?<TPayload>(
    destination: string,
    handler: (
      context: MessageContext<
        TPayload,
        TConsumeNative,
        TAcknowledgement
      >,
    ) => Promise<void> | void,
    options?: ConsumerOptions,
  ): Promise<QueueConsumer>;

  native?(): unknown;

  close?(): Promise<void> | void;
}
```

---

# 17. Strongly Typed Registry

Main configuration:

```ts
export type BuiltInQueueConfig =
  | KafkaConfig
  | RabbitMqConfig
  | BullMqConfig
  | RedisConfig
  | NatsConfig
  | SqsConfig
  | GooglePubSubConfig
  | AzureServiceBusConfig;
```

Factory:

```ts
export async function createQueue<
  const TConfig extends BuiltInQueueConfig,
>(
  config: TConfig,
): Promise<QueueForConfig<TConfig>>;
```

Example:

```ts
const kafka = await createQueue({
  type: 'kafka',

  brokers: [
    'localhost:9092',
  ],

  clientId: 'billing',
});
```

Then Kafka-native publish options should be inferred.

---

# 18. Strongly Typed Queue Registry

Allow applications to define messages once.

```ts
interface AppQueues {
  'emails.send': {
    payload: {
      userId: string;
      template: string;
    };
  };

  'payments.capture': {
    payload: {
      paymentId: string;
      amount: number;
    };
  };

  'image.resize': {
    payload: {
      imageId: string;
      width: number;
      height: number;
    };
  };
}
```

Then:

```ts
const queues = createTypedQueue<AppQueues>(provider);
```

Publish:

```ts
await queues.publish(
  'payments.capture',
  {
    paymentId: 'pay_123',
    amount: 250,
  },
);
```

Invalid payloads fail at compile time.

---

# 19. Event Registry

For stream/pubsub systems:

```ts
interface AppEvents {
  'user.created': {
    userId: string;
    email: string;
  };

  'order.completed': {
    orderId: string;
    total: number;
  };
}
```

Then:

```ts
events.publish(
  'user.created',
  {
    userId: 'u1',
    email: 'user@example.com',
  },
);
```

Consumer:

```ts
events.subscribe(
  'user.created',
  async ({ message }) => {
    console.log(message.payload.userId);
  },
);
```

---

# 20. Kafka Provider

Entrypoint:

```text
@mohamedhabibwork/queuekit/kafka
```

Recommended driver options:

```text
KafkaJS-compatible implementation initially
or a modern native Kafka client where runtime support allows
```

Do not lock the public API to one Kafka SDK.

Kafka capabilities:

```text
topics
partitions
consumer groups
offsets
headers
batch produce
batch consume
transactions
idempotent producer
manual commits
replay
retention
compression
```

Config:

```ts
export interface KafkaConfig {
  type: 'kafka';

  clientId: string;

  brokers: readonly string[];

  ssl?: boolean | object;

  sasl?: unknown;

  connectionTimeout?: number;

  requestTimeout?: number;

  native?: KafkaNativeConfig;
}
```

Publish:

```ts
await kafka.publish(
  'orders.completed',
  {
    payload: {
      orderId: 'ORD-1',
    },
  },
  {
    native: {
      partition: 1,

      headers: {
        traceId: 'abc',
      },
    },
  },
);
```

Consume:

```ts
await kafka.consume(
  'orders.completed',

  async ({
    message,
    ack,
  }) => {
    console.log(message.payload);

    await ack.complete();
  },

  {
    native: {
      groupId: 'billing-service',
      fromBeginning: false,
    },
  },
);
```

Kafka-specific helpers may include:

```ts
kafka.topic(...)
kafka.consumerGroup(...)
kafka.transaction(...)
kafka.admin()
```

Do not hide Kafka-native operations.

---

# 21. RabbitMQ Provider

Entrypoint:

```text
@mohamedhabibwork/queuekit/rabbitmq
```

Support:

```text
queues
exchanges
routing keys
direct exchange
topic exchange
fanout exchange
headers exchange
ack
nack
reject
prefetch
publisher confirms
dead-letter exchanges
TTL
priority
delayed exchange when plugin is available
```

Config:

```ts
export interface RabbitMqConfig {
  type: 'rabbitmq';

  url:
    | string
    | {
        hostname: string;
        port?: number;
        username?: string;
        password?: string;
        vhost?: string;
      };

  heartbeat?: number;

  native?: RabbitMqNativeConfig;
}
```

Queue publish:

```ts
await rabbit.publish(
  'emails',
  {
    payload: {
      userId: '123',
    },
  },
  {
    native: {
      persistent: true,
      expiration: '60000',
    },
  },
);
```

Exchange API:

```ts
await rabbit.exchange('events').publish(
  'order.completed',

  {
    orderId: 'ORD-1',
  },
);
```

Consumer:

```ts
await rabbit.consume(
  'emails',

  async ({
    message,
    ack,
  }) => {
    try {
      await sendEmail(message.payload);

      await ack.complete();
    } catch {
      await ack.retry();
    }
  },

  {
    native: {
      prefetch: 10,
    },
  },
);
```

---

# 22. BullMQ Provider

Use **BullMQ** as the modern Bull-family provider.

Entrypoint:

```text
@mohamedhabibwork/queuekit/bullmq
```

Capabilities:

```text
jobs
workers
delayed jobs
retries
backoff
priority
repeatable/scheduled jobs
rate limiting
flows
parent/child jobs
job progress
job logs
deduplication where supported
```

Config:

```ts
export interface BullMqConfig {
  type: 'bullmq';

  connection:
    | string
    | {
        host: string;
        port: number;
        username?: string;
        password?: string;
        db?: number;
      };

  prefix?: string;

  native?: BullMqNativeConfig;
}
```

Create queue:

```ts
const emails = await createQueue({
  type: 'bullmq',

  queue: 'emails',

  connection: {
    host: 'localhost',
    port: 6379,
  },
});
```

Publish:

```ts
await emails.publish(
  'emails',
  {
    type: 'welcome',

    payload: {
      userId: '123',
    },
  },
  {
    native: {
      attempts: 5,

      backoff: {
        type: 'exponential',
        delay: 1000,
      },

      removeOnComplete: 1000,

      removeOnFail: 5000,
    },
  },
);
```

Worker:

```ts
await emails.consume(
  'emails',

  async ({
    message,
  }) => {
    await sendWelcomeEmail(
      message.payload.userId,
    );
  },

  {
    concurrency: 20,

    native: {
      limiter: {
        max: 100,
        duration: 1000,
      },
    },
  },
);
```

Expose native BullMQ components:

```ts
bull.native().queue
bull.native().worker
bull.native().queueEvents
```

where applicable.

---

# 23. Redis Provider

Redis should not be treated as one single queue type.

Entrypoint:

```text
@mohamedhabibwork/queuekit/redis
```

Support three modes:

```text
redis-pubsub
redis-streams
redis-list
```

Config:

```ts
type RedisConfig =
  | RedisPubSubConfig
  | RedisStreamsConfig
  | RedisListQueueConfig;
```

---

## Redis Pub/Sub

```ts
const pubsub = await createQueue({
  type: 'redis',

  mode: 'pubsub',

  url: 'redis://localhost:6379',
});
```

Characteristics:

```text
ephemeral
no replay
no persistence guarantee
broadcast semantics
```

---

## Redis Streams

```ts
const stream = await createQueue({
  type: 'redis',

  mode: 'streams',

  url: 'redis://localhost:6379',
});
```

Support:

```text
XADD
XREAD
XREADGROUP
consumer groups
ack
pending messages
claiming
replay
retention/maxlen
```

---

## Redis List Queue

Basic list-backed jobs:

```text
LPUSH/RPUSH
BRPOP/BLPOP
```

Only expose this as a lightweight primitive.

Recommend BullMQ for advanced production job processing.

---

# 24. NATS Provider

Entrypoint:

```text
@mohamedhabibwork/queuekit/nats
```

Support:

```text
NATS Core
JetStream
request/reply
subjects
queue groups
durable consumers
acknowledgement
replay
retention
streams
```

Config:

```ts
export interface NatsConfig {
  type: 'nats';

  servers:
    | string
    | readonly string[];

  name?: string;

  token?: string;

  user?: string;

  pass?: string;

  mode?:
    | 'core'
    | 'jetstream';

  native?: NatsNativeOptions;
}
```

---

# 25. Amazon SQS Provider

Entrypoint:

```text
@mohamedhabibwork/queuekit/sqs
```

Support:

```text
standard queues
FIFO queues
delay
visibility timeout
long polling
batch send
batch receive
dead-letter queues
message attributes
message groups
deduplication IDs
```

Config:

```ts
export interface SqsConfig {
  type: 'sqs';

  region: string;

  queueUrl?: string;

  credentials?: unknown;

  endpoint?: string;

  native?: SqsNativeConfig;
}
```

Example FIFO:

```ts
await sqs.publish(
  queueUrl,
  {
    payload: order,
  },
  {
    native: {
      messageGroupId: 'orders',
      messageDeduplicationId: order.id,
    },
  },
);
```

---

# 26. Google Cloud Pub/Sub Provider

Entrypoint:

```text
@mohamedhabibwork/queuekit/google-pubsub
```

Support:

```text
topics
subscriptions
ack
nack
ack deadlines
ordering keys
dead-letter topics
retry policies
message attributes
```

---

# 27. Azure Service Bus Provider

Entrypoint:

```text
@mohamedhabibwork/queuekit/azure-service-bus
```

Support:

```text
queues
topics
subscriptions
sessions
scheduled enqueue
dead letter queue
peek lock
complete
abandon
defer
dead-letter
transactions
```

---

# 28. Custom Providers

Custom providers are a first-class design requirement.

Example:

```ts
import {
  defineQueueProvider,
} from '@mohamedhabibwork/queuekit/custom';

interface MyQueueConfig {
  endpoint: string;
  apiKey: string;
}

interface MyPublishOptions {
  priority?: 'low' | 'normal' | 'high';
}

interface MyResponse {
  id: string;
}

export const myQueueProvider =
  defineQueueProvider({
    name: 'my-queue',

    capabilities: {
      kind: 'queue',

      publish: true,
      consume: true,

      delayed: false,
      retries: true,
      ack: true,
    },

    async create(
      config: MyQueueConfig,
    ) {
      return {
        name: 'my-queue',

        capabilities: {
          kind: 'queue',
          publish: true,
          consume: true,
          retries: true,
          ack: true,
        },

        async publish(
          destination,
          message,
          options?: {
            native?: MyPublishOptions;
          },
        ) {
          const response = await fetch(
            `${config.endpoint}/queues/${destination}`,
            {
              method: 'POST',

              headers: {
                authorization:
                  `Bearer ${config.apiKey}`,

                'content-type':
                  'application/json',
              },

              body: JSON.stringify({
                message,
                ...options?.native,
              }),
            },
          );

          const native: MyResponse =
            await response.json();

          return {
            ok: response.ok,

            provider: 'my-queue',

            messageId: native.id,

            native,
          };
        },

        async close() {},
      };
    },
  });
```

Usage:

```ts
const provider =
  await myQueueProvider.create({
    endpoint:
      'https://queue.example.com',

    apiKey:
      process.env.QUEUE_API_KEY!,
  });
```

Custom providers should be able to support:

```text
internal MQs
company APIs
HTTP queues
gRPC queues
database queues
custom Redis protocols
legacy brokers
hosted services
```

without importing private QueueKit files.

---

# 29. Custom Provider Registry

Allow registering third-party providers.

```ts
const queuekit = createQueueKit({
  providers: {
    custommq: myQueueProvider,
  },
});
```

Then:

```ts
const queue =
  await queuekit.create({
    type: 'custommq',

    endpoint: '...',

    apiKey: '...',
  });
```

Type inference should remain intact.

---

# 30. Queue Manager

Provide a manager similar to StorageKit.

```ts
const queues =
  createQueueManager({
    default: 'jobs',

    providers: {
      jobs: {
        type: 'bullmq',

        connection: {
          host: 'localhost',
          port: 6379,
        },
      },

      events: {
        type: 'kafka',

        brokers: [
          'kafka-1:9092',
          'kafka-2:9092',
        ],

        clientId: 'api',
      },

      rabbit: {
        type: 'rabbitmq',

        url:
          'amqp://localhost',
      },

      cacheEvents: {
        type: 'redis',

        mode: 'streams',

        url:
          'redis://localhost:6379',
      },
    },
  });
```

Usage:

```ts
queues.provider('jobs');
queues.provider('events');
queues.provider('rabbit');
```

Types must resolve correctly:

```text
jobs -> BullMqProvider
events -> KafkaProvider
rabbit -> RabbitMqProvider
```

---

# 31. Multiple Connections of Same Provider

Must support:

```ts
const manager =
  createQueueManager({
    providers: {
      primaryKafka: {
        type: 'kafka',
        brokers: ['kafka-a:9092'],
        clientId: 'primary',
      },

      auditKafka: {
        type: 'kafka',
        brokers: ['kafka-b:9092'],
        clientId: 'audit',
      },

      jobsRedis: {
        type: 'bullmq',
        connection: jobsRedis,
      },

      notificationsRedis: {
        type: 'bullmq',
        connection: notificationsRedis,
      },
    },
  });
```

Aliases are independent from provider type.

---

# 32. Lazy Initialization

Connections should initialize lazily by default.

Benefits:

```text
faster startup
smaller cold start
unused SDKs remain unloaded
unused brokers are not contacted
fewer sockets
better serverless behavior
```

Example:

```ts
const kafka =
  await manager.provider(
    'primaryKafka',
  );
```

Optional warmup:

```ts
await manager.warmup();
```

---

# 33. Producer API

Generic producer:

```ts
const producer =
  queue.producer();
```

Publish:

```ts
await producer.publish(
  'orders',

  {
    payload: {
      orderId: '123',
    },
  },
);
```

Batch:

```ts
await producer.publishMany(
  'orders',

  [
    {
      payload: {
        orderId: '1',
      },
    },

    {
      payload: {
        orderId: '2',
      },
    },
  ],
);
```

Use provider-native batch APIs when possible.

---

# 34. Consumer API

```ts
const consumer =
  await queue.consume(
    'orders',

    async ({
      message,
      ack,
      signal,
    }) => {
      await processOrder(
        message.payload,
      );

      await ack.complete();
    },
  );
```

Return handle:

```ts
interface QueueConsumer {
  pause(): Promise<void>;

  resume(): Promise<void>;

  close(): Promise<void>;

  readonly status:
    | 'starting'
    | 'running'
    | 'paused'
    | 'closing'
    | 'closed';
}
```

---

# 35. Automatic Ack

Optional convenience:

```ts
await queue.consume(
  'orders',

  async ({ message }) => {
    await processOrder(
      message.payload,
    );
  },

  {
    autoAck: true,
  },
);
```

Semantics:

```text
handler resolves -> acknowledge
handler throws -> provider retry/failure behavior
```

Manual acknowledgement should remain available.

---

# 36. Retry Model

Normalized retry policy:

```ts
export interface RetryPolicy {
  attempts?: number;

  backoff?:
    | {
        type: 'fixed';
        delay: number;
      }
    | {
        type: 'exponential';
        delay: number;
        maxDelay?: number;
      };
}
```

Usage:

```ts
await queue.publish(
  'emails',

  message,

  {
    retry: {
      attempts: 5,

      backoff: {
        type: 'exponential',
        delay: 1000,
      },
    },
  },
);
```

Important:

Only map normalized retry policies when the provider genuinely supports them.

Otherwise:

```text
QueueUnsupportedFeatureError
```

or application-side middleware.

---

# 37. Dead Letter Queue

Provide configuration helpers:

```ts
deadLetter?: {
  destination: string;
  maxAttempts?: number;
}
```

But do not hide provider-native concepts.

Examples:

```text
RabbitMQ:
DLX + dead-letter routing key

SQS:
redrive policy

Azure:
built-in dead-letter subqueue

Google Pub/Sub:
dead-letter topic

Kafka:
typically application-level DLT topic

BullMQ:
failed jobs / custom dead-letter flow
```

Provider drivers translate only when semantics are meaningful.

---

# 38. Scheduling

Some providers support future delivery directly.

Normalized:

```ts
scheduleAt?: Date;
delay?: number;
```

Providers may implement:

```text
BullMQ delayed jobs
SQS delay up to provider limits
Azure scheduled enqueue
Rabbit delayed-message plugin
```

Kafka does not natively provide generic delayed delivery.

QueueKit should not simulate it invisibly.

---

# 39. Priority

Normalized priority:

```ts
priority?: number;
```

Only supported on providers that support meaningful priority semantics.

Examples:

```text
BullMQ
RabbitMQ priority queues
```

Do not pretend Kafka partitions are priority queues.

---

# 40. Serialization

Default codec:

```text
JSON
```

API:

```ts
interface QueueCodec<T> {
  encode(value: T):
    Uint8Array | string;

  decode(
    data:
      Uint8Array | string,
  ): T;
}
```

Built-in:

```text
json
text
bytes
```

Optional future:

```text
MessagePack
CBOR
Protobuf
Avro
```

Custom codec:

```ts
const queue =
  createQueue({
    ...config,

    codec: {
      encode,
      decode,
    },
  });
```

---

# 41. Schema Validation

QueueKit should not require a heavy schema library.

Optional typed validation:

```ts
const queues =
  createTypedQueue({
    schemas: {
      'user.created':
        userCreatedSchema,
    },
  });
```

Potential adapters:

```text
Zod
Valibot
ArkType
TypeBox
Standard Schema
JSON Schema
```

Schema validation should be optional.

---

# 42. Schema Registry Integration

Future optional adapters for Kafka ecosystems:

```text
Confluent Schema Registry
AWS Glue Schema Registry
Apicurio
custom registry
```

Keep this out of the core MVP.

---

# 43. Middleware

Middleware API:

```ts
queue.use(
  async (
    context,
    next,
  ) => {
    const started =
      performance.now();

    try {
      const result =
        await next();

      console.log({
        provider:
          context.provider,

        operation:
          context.operation,

        duration:
          performance.now()
          - started,
      });

      return result;
    } catch (error) {
      throw error;
    }
  },
);
```

Possible middleware:

```text
logging
OpenTelemetry
metrics
tracing
tenant resolution
idempotency
payload validation
encryption
compression
rate limiting
audit
```

---

# 44. Publish Context

```ts
interface QueueOperationContext {
  provider: string;

  providerAlias?: string;

  operation:
    | 'publish'
    | 'publishMany'
    | 'consume'
    | 'ack'
    | 'retry'
    | 'reject';

  destination: string;

  traceId?: string;

  correlationId?: string;

  metadata?:
    Record<string, unknown>;
}
```

---

# 45. Message Metadata vs Broker Headers

Keep application metadata separate.

```ts
message.metadata
```

should not automatically be sent to provider.

Provider-delivered headers:

```ts
message.headers
```

This distinction prevents internal telemetry metadata from leaking into messages.

---

# 46. Correlation and Causation

Built-in support:

```ts
correlationId?: string;
causationId?: string;
traceId?: string;
```

These may be mapped to provider headers when configured.

Example:

```ts
queue.use(
  createTracingMiddleware({
    propagate: true,
  }),
);
```

---

# 47. Idempotency

Expose:

```ts
idempotencyKey?: string;
```

Map natively where providers support it.

Examples:

```text
SQS FIFO deduplication
BullMQ custom job IDs
some custom providers
```

For others:

```text
application middleware + Redis/DB store
```

Do not promise exactly-once delivery globally.

---

# 48. Delivery Guarantees

QueueKit docs must clearly distinguish:

```text
at-most-once
at-least-once
effectively-once
provider transactional guarantees
```

Never market QueueKit itself as:

```text
exactly once across all providers
```

because that is not a realistic universal guarantee.

---

# 49. Ordering

Capability:

```ts
ordering?: boolean;
```

Provider semantics differ:

```text
Kafka -> partition ordering
SQS FIFO -> message group ordering
Azure Sessions -> ordered processing
RabbitMQ -> queue delivery order with caveats
BullMQ -> job order affected by priority/concurrency
Redis Streams -> stream ID ordering
```

Expose native ordering controls.

---

# 50. Transactions

Optional API:

```ts
if (
  queue.capabilities.transactions
) {
  await queue.transaction(
    async (tx) => {
      await tx.publish(...);
      await tx.publish(...);
    },
  );
}
```

Initially implement for providers where transaction semantics are strong and useful.

Example:

```text
Kafka transactions
```

Do not fake transactions on providers without them.

---

# 51. Kafka Transaction Example

```ts
await kafka.transaction(
  async (tx) => {
    await tx.publish(
      'payments.completed',
      payment,
    );

    await tx.publish(
      'audit.events',
      auditEvent,
    );
  },
);
```

Native transaction object should remain accessible.

---

# 52. Request/Reply

Useful for providers such as:

```text
NATS
RabbitMQ
```

Optional interface:

```ts
const response =
  await queue.request(
    'users.lookup',

    {
      userId: '123',
    },

    {
      timeout: 5000,
    },
  );
```

Do not expose this as mandatory provider behavior.

Capability:

```ts
requestReply?: boolean;
```

---

# 53. Streams API

For Kafka, Redis Streams, JetStream, Pulsar:

```ts
const stream =
  queue.stream(
    'events',
  );
```

API:

```ts
await stream.append(event);

await stream.consume(...);

await stream.replay({
  from: ...,
});
```

This can be a separate higher-level interface.

Do not overload the basic Job Queue API.

---

# 54. Pub/Sub API

```ts
const topic =
  queue.topic(
    'user-events',
  );

await topic.publish({
  type: 'user.created',
  payload: {
    userId: '1',
  },
});

await topic.subscribe(
  async ({ message }) => {
    ...
  },
);
```

Provider examples:

```text
Redis Pub/Sub
NATS
Google Pub/Sub
SNS
RabbitMQ exchange
```

---

# 55. Job Queue API

```ts
const jobs =
  queue.jobs(
    'images',
  );

await jobs.add(
  'resize',

  {
    imageId: 'img_1',
  },

  {
    delay: 5000,
    attempts: 3,
  },
);
```

Worker:

```ts
await jobs.worker(
  'resize',

  async (job) => {
    ...
  },
);
```

This API is ideal for:

```text
BullMQ
```

and adapters with similar semantics.

---

# 56. Native Escape Hatch

Every provider should expose its underlying SDK/client.

Examples:

```ts
kafka.native()
rabbit.native()
bull.native()
redis.native()
nats.native()
sqs.native()
```

Possible results:

```text
Kafka client / producer / consumer
RabbitMQ connection/channel
BullMQ Queue/Worker
Redis client
NATS connection
AWS SQS client
```

QueueKit must not block advanced provider-specific features.

---

# 57. Optional Dependencies

Provider SDKs must be optional peer dependencies.

Conceptual example:

```json
{
  "peerDependencies": {
    "bullmq": "^...",
    "ioredis": "^...",
    "kafkajs": "^...",
    "amqplib": "^...",
    "redis": "^...",
    "nats": "^...",
    "@aws-sdk/client-sqs": "^...",
    "@google-cloud/pubsub": "^...",
    "@azure/service-bus": "^..."
  },
  "peerDependenciesMeta": {
    "bullmq": {
      "optional": true
    },
    "ioredis": {
      "optional": true
    },
    "kafkajs": {
      "optional": true
    },
    "amqplib": {
      "optional": true
    },
    "redis": {
      "optional": true
    },
    "nats": {
      "optional": true
    },
    "@aws-sdk/client-sqs": {
      "optional": true
    },
    "@google-cloud/pubsub": {
      "optional": true
    },
    "@azure/service-bus": {
      "optional": true
    }
  }
}
```

Do not load every SDK from the root entrypoint.

---

# 58. Missing Provider Dependency Error

Example:

```text
QueueConfigError:

The "bullmq" provider requires the
"bullmq" package.

Install:

npm install bullmq

or:

bun add bullmq

or:

deno add npm:bullmq
```

Provider initialization should fail only when that provider is used.

---

# 59. Runtime-Neutral Core

Core may depend on:

```text
Promise
AbortController
AbortSignal
URL
TextEncoder
TextDecoder
crypto
performance
structuredClone
```

Avoid core dependencies on:

```text
node:fs
node:path
node:net
node:tls
Buffer
process
```

Provider implementations may use them where upstream protocols require Node compatibility.

---

# 60. Abort / Cancellation

Producer:

```ts
await queue.publish(
  destination,
  message,
  {
    signal,
  },
);
```

Consumers receive:

```ts
context.signal
```

Graceful shutdown should abort in-flight consumers where appropriate.

---

# 61. Timeouts

Normalized options:

```ts
timeout?: number;
```

Map to provider-native request timeouts where possible.

Do not confuse:

```text
operation timeout
visibility timeout
message TTL
consumer lock duration
```

These remain distinct concepts.

---

# 62. Batch Publishing

```ts
await queue.publishMany(
  'events',
  messages,
);
```

Drivers should use native batch APIs:

```text
Kafka producer batches
SQS SendMessageBatch
Google Pub/Sub batching
RabbitMQ confirm channel batching
```

BullMQ can use:

```text
addBulk
```

where applicable.

---

# 63. Batch Consumption

Expose only when it makes sense.

```ts
consumeBatch?(
  destination,
  handler,
  options,
)
```

Examples:

```text
Kafka batches
SQS receive batches
```

Do not emulate batch consumption by buffering unless explicitly configured.

---

# 64. Concurrency

Generic consumer option:

```ts
interface ConsumerOptions<
  TNative = unknown,
> {
  concurrency?: number;

  autoAck?: boolean;

  timeout?: number;

  native?: TNative;
}
```

Provider translation differs.

Examples:

```text
BullMQ -> worker concurrency
RabbitMQ -> prefetch + handler concurrency
SQS -> number of parallel handlers
Kafka -> partition-aware concurrency
```

---

# 65. Backpressure

QueueKit must respect provider flow-control where available.

Examples:

```text
Kafka consumer pause/resume
RabbitMQ channel backpressure
NATS pending limits
Node stream-style producer pressure
```

Consumer handle:

```ts
await consumer.pause();
await consumer.resume();
```

---

# 66. Graceful Shutdown

```ts
await manager.close();
```

Recommended shutdown order:

```text
stop accepting new jobs
pause consumers
wait for in-flight handlers
commit/ack completed messages
close consumers
flush producers
close broker connections
```

Config:

```ts
await manager.close({
  timeout: 30_000,
});
```

---

# 67. Error Model

```ts
export class QueueError
  extends Error {}

export class QueueConfigError
  extends QueueError {}

export class QueueConnectionError
  extends QueueError {}

export class QueueAuthenticationError
  extends QueueError {}

export class QueueAuthorizationError
  extends QueueError {}

export class QueuePublishError
  extends QueueError {}

export class QueueConsumeError
  extends QueueError {}

export class QueueTimeoutError
  extends QueueError {}

export class QueueRateLimitError
  extends QueueError {}

export class QueueSerializationError
  extends QueueError {}

export class QueueDeserializationError
  extends QueueError {}

export class QueueUnsupportedFeatureError
  extends QueueError {}

export class QueueClosedError
  extends QueueError {}
```

---

# 68. Error Context

```ts
interface QueueErrorContext {
  provider: string;

  operation?: string;

  destination?: string;

  code?: string;

  retryable?: boolean;

  statusCode?: number;

  native?: unknown;

  cause?: unknown;
}
```

Preserve provider-native errors.

---

# 69. Retryable Errors

Helper:

```ts
if (
  isRetryableQueueError(
    error,
  )
) {
  ...
}
```

Classify:

```text
network failures
temporary broker unavailable
rate limiting
consumer rebalance-related transient errors
provider service unavailable
```

Permanent errors:

```text
invalid credentials
invalid destination
schema violation
malformed configuration
permission denied
```

---

# 70. Health Checks

Optional:

```ts
const health =
  await queue.health();
```

Result:

```ts
interface QueueHealth {
  ok: boolean;

  provider: string;

  latencyMs?: number;

  native?: unknown;
}
```

Manager:

```ts
await manager.health();
```

Returns health per provider alias.

---

# 71. Observability

Built-in integration hooks should support:

```text
OpenTelemetry
metrics
structured logs
traces
```

Suggested metrics:

```text
queue_publish_total
queue_publish_failed_total
queue_consume_total
queue_consume_failed_total
queue_handler_duration_ms
queue_publish_duration_ms
queue_retry_total
queue_dead_letter_total
```

Do not include full payloads by default.

---

# 72. Security

Never log:

```text
passwords
connection strings with credentials
AWS credentials
Kafka SASL passwords
RabbitMQ passwords
Redis passwords
TLS private keys
API tokens
full message bodies by default
```

Provide redaction helpers.

---

# 73. Encryption Middleware

Optional future middleware:

```ts
queue.use(
  createEncryptionMiddleware({
    encrypt,
    decrypt,
  }),
);
```

Should run before serialization/publish and after receive/decode.

Keep encryption outside core transport semantics.

---

# 74. Testing Provider

Provide in-memory test implementation.

```ts
const queue =
  createMemoryQueue();
```

Publish:

```ts
await queue.publish(
  'emails',

  {
    payload: {
      userId: '123',
    },
  },
);
```

Inspect:

```ts
queue.messages('emails');
queue.lastMessage('emails');
queue.clear();
```

Consume:

```ts
await queue.consume(
  'emails',

  async ({
    message,
  }) => {
    ...
  },
);
```

Testing controls:

```ts
queue.failNext(...)
queue.setLatency(...)
queue.pause(...)
queue.resume(...)
```

---

# 75. Contract Test Suite

Every provider should pass common tests.

Examples:

```text
publishes a message
preserves native response
serializes payload
deserializes payload
supports cancellation
maps connection errors
maps auth errors
does not leak credentials
closes idempotently
exposes capabilities
supports multiple consumers where valid
handles malformed payloads
```

Provider-specific tests then verify native features.

---

# 76. Type Tests

Critical tests:

Kafka:

```ts
const kafka =
  await createQueue({
    type: 'kafka',
    clientId: 'app',
    brokers: ['localhost:9092'],
  });

await kafka.publish(
  'events',
  message,
  {
    native: {
      partition: 1,
    },
  },
);
```

should compile.

This should fail:

```ts
await kafka.publish(
  'events',
  message,
  {
    native: {
      attempts: 5,
    },
  },
);
```

because `attempts` is a BullMQ-style option.

BullMQ should accept:

```ts
native: {
  attempts: 5,
  backoff: {
    type: 'exponential',
    delay: 1000,
  },
}
```

---

# 77. TypeScript CI Matrix

```yaml
strategy:
  matrix:
    typescript:
      - "5.9"
      - "6"
      - "7"
```

Run:

```text
build
type tests
declaration tests
sample consumer projects
```

---

# 78. Runtime CI

Test:

```text
Node 20
Node 22+
latest Node LTS

Bun stable

Deno 2.x
```

Core/fake tests should run on all.

Integration providers run according to upstream compatibility.

---

# 79. Integration Test Infrastructure

Docker Compose test services:

```text
Kafka / Redpanda
RabbitMQ
Redis
NATS
```

Cloud provider tests:

```text
SQS -> optional LocalStack + real AWS protected tests
Google Pub/Sub -> emulator
Azure Service Bus -> mocked/unit + protected integration
```

---

# 80. Kafka Test Broker

For local CI, Redpanda can be useful as a Kafka-compatible test broker.

Tests should still validate against Kafka-compatible behavior rather than relying on Redpanda-specific APIs.

---

# 81. Suggested Build

```text
ESM
target ES2022
declaration output
sideEffects false
tree-shakable
```

Builder:

```text
tsup
```

or:

```text
tsdown
```

Keep build tooling replaceable.

---

# 82. Suggested package.json

```json
{
  "name": "@mohamedhabibwork/queuekit",
  "version": "0.1.0",
  "type": "module",
  "sideEffects": false,
  "engines": {
    "node": ">=20"
  },
  "files": [
    "dist",
    "README.md",
    "LICENSE"
  ]
}
```

Deno and Bun support should be documented/tested instead of using npm `engines`.

---

# 83. Example — BullMQ

```ts
import {
  createQueue,
} from '@mohamedhabibwork/queuekit';

const queue =
  await createQueue({
    type: 'bullmq',

    queue: 'emails',

    connection: {
      host: 'localhost',
      port: 6379,
    },
  });

await queue.publish(
  'emails',

  {
    type: 'welcome',

    payload: {
      userId: '123',
      email: 'user@example.com',
    },
  },

  {
    native: {
      attempts: 5,

      backoff: {
        type: 'exponential',
        delay: 1000,
      },

      removeOnComplete: true,
    },
  },
);
```

---

# 84. Example — Kafka

```ts
const kafka =
  await createQueue({
    type: 'kafka',

    clientId:
      'orders-service',

    brokers: [
      'kafka-1:9092',
      'kafka-2:9092',
    ],
  });

await kafka.publish(
  'orders.completed',

  {
    payload: {
      orderId:
        'ORD-1001',

      total:
        750,
    },

    correlationId:
      'request-123',
  },

  {
    native: {
      partition: 3,

      headers: {
        source:
          'orders-api',
      },
    },
  },
);
```

---

# 85. Example — RabbitMQ

```ts
const rabbit =
  await createQueue({
    type: 'rabbitmq',

    url:
      'amqp://localhost',
  });

await rabbit.publish(
  'emails',

  {
    payload: {
      email:
        'user@example.com',

      template:
        'welcome',
    },
  },

  {
    native: {
      persistent: true,

      priority: 5,
    },
  },
);
```

---

# 86. Example — Redis Streams

```ts
const redis =
  await createQueue({
    type: 'redis',

    mode: 'streams',

    url:
      'redis://localhost:6379',

    group:
      'billing',

    consumer:
      'worker-1',
  });

await redis.publish(
  'payments',

  {
    payload: {
      paymentId:
        'pay_1',
    },
  },
);
```

---

# 87. Example — SQS

```ts
const sqs =
  await createQueue({
    type: 'sqs',

    region:
      'eu-central-1',
  });

await sqs.publish(
  queueUrl,

  {
    payload: {
      taskId:
        'task-100',
    },
  },

  {
    native: {
      delaySeconds:
        30,
    },
  },
);
```

---

# 88. Example — Custom Provider

```ts
const provider =
  defineQueueProvider({
    name:
      'internal-mq',

    capabilities: {
      kind:
        'queue',

      publish:
        true,

      consume:
        false,
    },

    async create(
      config:
        InternalConfig,
    ) {
      return {
        name:
          'internal-mq',

        capabilities: {
          kind:
            'queue',

          publish:
            true,

          consume:
            false,
        },

        async publish(
          destination,
          message,
        ) {
          const res =
            await fetch(
              `${config.baseUrl}/${destination}`,
              {
                method:
                  'POST',

                body:
                  JSON.stringify(
                    message,
                  ),
              },
            );

          const native =
            await res.json();

          return {
            ok:
              res.ok,

            provider:
              'internal-mq',

            messageId:
              native.id,

            native,
          };
        },
      };
    },
  });
```

---

# 89. Integration with NotificationKit

QueueKit should integrate naturally with NotificationKit.

Producer:

```ts
await queues.publish(
  'notifications.send',

  {
    payload: {
      provider:
        'email',

      recipient:
        user.email,

      template:
        'welcome',
    },
  },
);
```

Worker:

```ts
await queues.consume(
  'notifications.send',

  async ({
    message,
  }) => {
    await notifications
      .provider(
        message.payload.provider,
      )
      .send(...);
  },
);
```

Neither package should depend directly on the other.

Create optional integration packages/helpers later if useful.

---

# 90. Integration with StorageKit

Large messages should not automatically be stored externally by QueueKit.

But an optional pattern can use StorageKit:

```ts
const object =
  await storage.put(
    largePayload,
  );

await queue.publish(
  'video.process',

  {
    payload: {
      storageKey:
        object.key,
    },
  },
);
```

A future helper may implement:

```text
large-message offloading
```

using:

```text
@mohamedhabibwork/storagekit
```

without coupling the QueueKit core.

---

# 91. Large Payload Middleware

Optional future integration:

```ts
queue.use(
  createLargePayloadMiddleware({
    storage,
    threshold:
      200_000,
  }),
);
```

Behavior:

```text
small payload -> broker normally

large payload ->
store in StorageKit
publish reference
consumer retrieves object
```

This should never happen implicitly.

---

# 92. Recommended Provider Priority

## v0.1

Core:

```text
core contracts
provider registry
factory
manager
testing provider
custom provider API
middleware
errors
serialization
```

Providers:

```text
BullMQ
Kafka
RabbitMQ
Redis Streams
Redis Pub/Sub
```

Why:

```text
covers job queue
event streaming
traditional MQ
lightweight pub/sub
```

---

# 93. v0.2

Add:

```text
NATS
Amazon SQS
batch APIs
dead-letter abstractions
health checks
observability
```

---

# 94. v0.3

Add:

```text
Google Pub/Sub
Azure Service Bus
typed event registry
typed queue registry
request/reply
transactions
```

---

# 95. v0.4

Add:

```text
Amazon SNS
Pulsar
Redpanda-specific enhancements
Cloudflare Queues
Upstash
generic HTTP provider
```

---

# 96. v1.0 Requirements

Do not release `1.0` until:

```text
provider contract is stable
custom provider contract is stable
error model is stable
manager typing is stable
native option typing is stable
consumer lifecycle API is stable
ack semantics are documented
retry semantics are documented
delivery guarantee docs are complete
```

---

# 97. Implementation Order

Recommended sequence:

1. Define message types.
2. Define publish result.
3. Define consumer context.
4. Define acknowledgement API.
5. Define provider capabilities.
6. Define provider contract.
7. Build fake/memory provider.
8. Build contract test suite.
9. Build factory.
10. Build typed provider registry.
11. Build QueueManager.
12. Build serializer/codec layer.
13. Build middleware.
14. Implement BullMQ.
15. Implement Kafka.
16. Implement RabbitMQ.
17. Implement Redis Streams.
18. Implement Redis Pub/Sub.
19. Add batch publish.
20. Add retries/dead-letter helpers.
21. Implement NATS.
22. Implement SQS.
23. Add typed queue/event registries.
24. Add observability.
25. Implement Google Pub/Sub.
26. Implement Azure Service Bus.
27. Add runtime CI.
28. Add TS 5.9/6/7 matrix.
29. Stabilize custom provider API.
30. Prepare v1.

---

# 98. Architecture Rules

## Rule 1 — Do not flatten provider-native options

Bad:

```ts
interface QueueOptions {
  partition?: number;
  attempts?: number;
  routingKey?: string;
  messageGroupId?: string;
}
```

Good:

```ts
interface PublishOptions<
  TNative,
> {
  delay?: number;
  priority?: number;
  native?: TNative;
}
```

---

## Rule 2 — Core must not import broker SDKs

Bad:

```ts
import 'bullmq';
import 'kafkajs';
import 'amqplib';
```

inside root/core.

Good:

```text
provider entrypoint loads provider dependency
```

---

## Rule 3 — Preserve native responses

Normalized:

```ts
result.ok
result.messageId
```

Native:

```ts
result.native
```

---

## Rule 4 — Preserve native consumed message

```ts
context.message.native
```

---

## Rule 5 — Preserve native acknowledgement

```ts
context.ack.native
```

where appropriate.

---

## Rule 6 — Custom providers must use only public APIs

No custom driver should import:

```text
src/internal/*
```

---

## Rule 7 — Queue family matters

Do not model:

```text
Kafka == BullMQ == RabbitMQ
```

They share infrastructure contracts but have different semantics.

---

## Rule 8 — No fake universal exactly-once guarantee

Document real guarantees.

---

## Rule 9 — Runtime-neutral core

Node-specific APIs remain inside provider drivers.

---

## Rule 10 — Strong types are mandatory

Avoid:

```ts
any
```

in public APIs.

Prefer:

```ts
unknown
generics
conditional types
mapped types
discriminated unions
```

---

# 99. Recommended README Description

```text
QueueKit is a runtime-neutral TypeScript queue and messaging toolkit for Node.js, Bun, and Deno.

Use Kafka, RabbitMQ, BullMQ, Redis, NATS, SQS, Google Pub/Sub,
Azure Service Bus, and custom providers through one architecture
without losing strongly typed provider-native options.
```

---

# 100. Example Final Developer Experience

```ts
import {
  createQueueManager,
} from '@mohamedhabibwork/queuekit';

const queues =
  createQueueManager({
    default:
      'jobs',

    providers: {
      jobs: {
        type:
          'bullmq',

        connection: {
          host:
            'localhost',

          port:
            6379,
        },
      },

      events: {
        type:
          'kafka',

        clientId:
          'api',

        brokers: [
          'kafka:9092',
        ],
      },

      messaging: {
        type:
          'rabbitmq',

        url:
          'amqp://rabbitmq',
      },

      streams: {
        type:
          'redis',

        mode:
          'streams',

        url:
          'redis://redis:6379',
      },
    },
  });

await queues
  .provider('jobs')
  .publish(
    'emails',

    {
      payload: {
        userId:
          '123',
      },
    },

    {
      native: {
        attempts:
          5,

        backoff: {
          type:
            'exponential',

          delay:
            1000,
        },
      },
    },
  );

await queues
  .provider('events')
  .publish(
    'user.created',

    {
      payload: {
        userId:
          '123',
      },
    },

    {
      native: {
        partition:
          2,

        headers: {
          source:
            'api',
        },
      },
    },
  );

await queues
  .provider('messaging')
  .publish(
    'payments',

    {
      payload: {
        paymentId:
          'pay_123',
      },
    },

    {
      native: {
        persistent:
          true,
      },
    },
  );
```

At each call site, TypeScript should know:

```text
valid provider config
valid destination semantics
valid message payload
valid native publish options
valid native consumer options
valid native acknowledgement type
valid provider-native response
```

without developers manually passing generic parameters.

---

# 101. Definition of Done

QueueKit is ready for stable release when:

- Node.js support passes CI.
- Bun support passes CI.
- Deno support passes CI.
- TypeScript 5.9 passes.
- TypeScript 6.x passes.
- TypeScript 7.x passes.
- BullMQ provider works.
- Kafka provider works.
- RabbitMQ provider works.
- Redis Streams works.
- Redis Pub/Sub works.
- NATS works.
- SQS works.
- provider SDKs remain optional.
- root import does not load provider SDKs.
- native config remains strongly typed.
- native publish options remain strongly typed.
- native consumer options remain strongly typed.
- native responses are preserved.
- native consumed messages are preserved.
- custom provider API is public and documented.
- manager supports multiple named connections.
- consumers support graceful shutdown.
- acknowledgement semantics are documented.
- retry semantics are documented.
- delivery guarantees are documented.
- memory/testing provider exists.
- provider contract tests exist.
- no credentials leak into logs/errors.
- public API contains no unexplained `any`.
- provider capability detection works.
- README includes npm, Bun, and Deno examples.

---

# 102. Recommended Package Tagline

```text
Unified TypeScript queue, messaging, pub/sub, and streaming infrastructure across Kafka, RabbitMQ, BullMQ, Redis, NATS, cloud MQs, and custom providers — without sacrificing native provider capabilities or type safety.
```
