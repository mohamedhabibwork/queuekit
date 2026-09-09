# QueueKit

[![npm version](https://img.shields.io/npm/v/@mohamedhabibwork/queuekit)](https://www.npmjs.com/package/@mohamedhabibwork/queuekit)
[![npm downloads](https://img.shields.io/npm/dm/@mohamedhabibwork/queuekit)](https://www.npmjs.com/package/@mohamedhabibwork/queuekit)
[![Latest Release](https://img.shields.io/github/v/release/mohamedhabibwork/queuekit)](https://github.com/mohamedhabibwork/queuekit/releases/latest)
[![License: MIT](https://img.shields.io/npm/l/@mohamedhabibwork/queuekit)](./LICENSE)
[![GitHub: @mohamedhabibwork](https://img.shields.io/badge/GitHub-@mohamedhabibwork-181717?logo=github&logoColor=white)](https://github.com/mohamedhabibwork)
[![Node.js >= 20](https://img.shields.io/node/v/@mohamedhabibwork/queuekit)](https://nodejs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org)
[![CI](https://github.com/mohamedhabibwork/queuekit/actions/workflows/ci.yml/badge.svg)](https://github.com/mohamedhabibwork/queuekit/actions/workflows/ci.yml)
[![Docs](https://github.com/mohamedhabibwork/queuekit/actions/workflows/pages.yml/badge.svg)](https://mohamedhabibwork.github.io/queuekit/)
[![TypeScript compatibility](https://github.com/mohamedhabibwork/queuekit/actions/workflows/typescript.yml/badge.svg)](https://github.com/mohamedhabibwork/queuekit/actions/workflows/typescript.yml)
[![Socket](https://badge.socket.dev/npm/package/@mohamedhabibwork/queuekit)](https://socket.dev/npm/package/@mohamedhabibwork/queuekit)

Runtime-neutral TypeScript infrastructure for job queues, message queues, pub/sub, and streams—without erasing provider-native capabilities and types.

QueueKit supports Node.js 20+, Bun, and Deno 2+ for its runtime-neutral core. Version `0.1` ships adapters for BullMQ, Kafka, RabbitMQ, Redis (Pub/Sub and Streams), NATS Core, and Amazon SQS. Each SDK is an optional peer dependency and loads only when that provider is created.

## Install

```sh
npm install @mohamedhabibwork/queuekit
# Add only the provider SDK you use, for example:
npm install kafkajs
```

```sh
bun add @mohamedhabibwork/queuekit kafkajs
```

```sh
deno add npm:@mohamedhabibwork/queuekit
deno add npm:kafkajs
```

## Usage

```ts
import { createQueue } from '@mohamedhabibwork/queuekit';

const kafka = await createQueue({
  type: 'kafka',
  clientId: 'orders-api',
  brokers: ['localhost:9092'],
});

await kafka.publish('orders.created', {
  payload: { orderId: 'ord_123' },
}, {
  native: { partition: 2, headers: { source: 'api' } },
});
```

Provider-native options are intentionally kept under `native`; BullMQ options cannot accidentally be passed to Kafka and vice versa.

```ts
import { createQueueManager } from '@mohamedhabibwork/queuekit';

const queues = createQueueManager({
  default: 'jobs',
  providers: {
    jobs: { type: 'bullmq', connection: { host: 'localhost', port: 6379 } },
    events: { type: 'kafka', clientId: 'api', brokers: ['localhost:9092'] },
  },
});

await (await queues.provider('jobs')).publish('emails', {
  type: 'welcome',
  payload: { userId: 'u_1' },
}, { native: { attempts: 5, backoff: { type: 'exponential', delay: 1_000 } } });
```

## Providers

| Provider | Entrypoint | Family |
| --- | --- | --- |
| BullMQ | `@mohamedhabibwork/queuekit/bullmq` | Job queue |
| Kafka | `@mohamedhabibwork/queuekit/kafka` | Event stream |
| RabbitMQ | `@mohamedhabibwork/queuekit/rabbitmq` | Message queue |
| Redis | `@mohamedhabibwork/queuekit/redis` | Pub/Sub or stream |
| NATS | `@mohamedhabibwork/queuekit/nats` | Pub/Sub; JetStream publishing |
| Amazon SQS | `@mohamedhabibwork/queuekit/sqs` | Message queue |

See the [documentation site](https://mohamedhabibwork.github.io/queuekit/) for capabilities and acknowledgement semantics.

## Custom providers and tests

```ts
import { defineQueueProvider } from '@mohamedhabibwork/queuekit/custom';
import { createMemoryQueue } from '@mohamedhabibwork/queuekit/testing';

const testQueue = createMemoryQueue();

const provider = defineQueueProvider({
  name: 'internal' as const,
  capabilities: { kind: 'queue', publish: true, consume: false },
  async create(config: { endpoint: string }) {
    // Return a QueueProvider implemented entirely against public QueueKit types.
    return testQueue;
  },
});
```

## Guarantees

QueueKit does not claim universal exactly-once delivery. Delivery, retry, ordering, acknowledgement, and dead-letter behavior are broker-specific; use `capabilities`, provider-native configuration, and idempotent consumers. `message.metadata` is application-only and is never implicitly sent to a broker.

## Development

```sh
npm install --legacy-peer-deps
npm run check
npm pack --dry-run
```

The CI matrix tests Node 20, 22, 24, and 26; Bun; Deno; and TypeScript 5.9, 6, and 7. Local Node 22 and Bun checks are run before release; Deno is covered in GitHub Actions when it is not installed locally.

## Publishing

Publishing runs only from the Release workflow. Add an npm automation token as the repository Actions secret `NPM_TOKEN`; a local `.env` file cannot be read by GitHub-hosted runners. Details are in [docs/publishing.md](docs/publishing.md).

## License

[MIT](LICENSE)
