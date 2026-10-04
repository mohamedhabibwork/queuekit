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

QueueKit supports Node.js 20+, Bun, and Deno 2+ for its runtime-neutral core. Version `0.1` ships adapters for BullMQ, Kafka, RabbitMQ, Redis (Pub/Sub and Streams), NATS Core, Amazon SQS, Cloudflare Queues, Google Cloud Pub/Sub, and Azure Service Bus. Each SDK is an optional peer dependency and loads only when that provider is created (the Cloudflare driver needs no SDK — it speaks the Queues REST API over `fetch`).

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

## Framework integration

QueueKit is framework-agnostic: enqueue with `publish()`, consume with a plain async loop. Ready-made recipes:

| Framework            | Recipe                     |
| -------------------- | -------------------------- |
| Express 4/5          | enqueue from routes        |
| Fastify 4/5          | plugin with graceful close |
| NestJS 10+           | injectable queue service   |
| Hono 4               | route handler enqueue      |
| Next.js (App Router) | server action + worker     |
| Elysia (Bun)         | shared async instance      |

See [framework integration](docs/frameworks.md) and [end-to-end examples](docs/examples.md) for copy-paste snippets.

**[Use cases](docs/use-cases.md)** — 26 recipes with examples: delayed/scheduled/recurring jobs, retries with backoff (`withRetry`), dead letters, handler timeouts (`withTimeout`), idempotent consumers (`withIdempotency`), deduplicated publishing, batching, fan-out, backpressure, graceful shutdown (`gracefulShutdown`), middleware, typed registries, and testing.

## Usage

```ts
import { createQueue } from "@mohamedhabibwork/queuekit";

const kafka = await createQueue({
  type: "kafka",
  clientId: "orders-api",
  brokers: ["localhost:9092"],
});

await kafka.publish(
  "orders.created",
  {
    payload: { orderId: "ord_123" },
  },
  {
    native: { partition: 2, headers: { source: "api" } },
  },
);
```

Provider-native options are intentionally kept under `native`; BullMQ options cannot accidentally be passed to Kafka and vice versa.

```ts
import { createQueueManager } from "@mohamedhabibwork/queuekit";

const queues = createQueueManager({
  default: "jobs",
  providers: {
    jobs: { type: "bullmq", connection: { host: "localhost", port: 6379 } },
    events: { type: "kafka", clientId: "api", brokers: ["localhost:9092"] },
  },
});

await (
  await queues.provider("jobs")
).publish(
  "emails",
  {
    type: "welcome",
    payload: { userId: "u_1" },
  },
  { native: { attempts: 5, backoff: { type: "exponential", delay: 1_000 } } },
);
```

## Providers

| Provider             | Entrypoint                                   | Family                                      |
| -------------------- | -------------------------------------------- | ------------------------------------------- |
| BullMQ               | `@mohamedhabibwork/queuekit/bullmq`          | Job queue                                   |
| Kafka                | `@mohamedhabibwork/queuekit/kafka`           | Event stream                                |
| RabbitMQ             | `@mohamedhabibwork/queuekit/rabbitmq`        | Message queue                               |
| Redis                | `@mohamedhabibwork/queuekit/redis`           | Pub/Sub or stream                           |
| NATS                 | `@mohamedhabibwork/queuekit/nats`            | Pub/Sub; JetStream publishing               |
| Amazon SQS           | `@mohamedhabibwork/queuekit/sqs`             | Message queue                               |
| Cloudflare Queues    | `@mohamedhabibwork/queuekit/cloudflare`      | Message queue (REST API or Workers binding) |
| Google Cloud Pub/Sub | `@mohamedhabibwork/queuekit/gcpubsub`        | Pub/sub                                     |
| Azure Service Bus    | `@mohamedhabibwork/queuekit/azureservicebus` | Message queue                               |

See the [documentation site](https://mohamedhabibwork.github.io/queuekit/) for capabilities and acknowledgement semantics. BullMQ no longer bundles a Redis client: when `connection` is a URL string, also install `ioredis` (`npm install bullmq ioredis`). The Redis provider works with both `node-redis` (default) and `ioredis` (`client: 'ioredis'`), against Redis and Valkey servers, and Streams consumers can dead-letter rejected entries with `native.deadLetter`.

The cloud drivers keep provider-native handles under `native`: SQS/Service Bus expose the SDK client, Pub/Sub the `PubSub` instance, and Cloudflare the optional Workers binding. Cloudflare Queues works from any runtime — publish over the REST API (`accountId` + `apiToken`) or through a Workers binding (`binding: env.MY_QUEUE`) inside a Worker, and consume with the pull API:

```ts
import { createQueue } from "@mohamedhabibwork/queuekit";

const queue = await createQueue({
  type: "cloudflare",
  queueId: "your-queue-id",
  accountId: process.env.CF_ACCOUNT_ID,
  apiToken: process.env.CF_QUEUES_TOKEN, // Queues read + write
});

await queue.publish("orders", { payload: { orderId: "ord_9" } }, { delay: 30_000 });

const consumer = await queue.consume("orders", async ({ message, ack }) => {
  await fulfill(message.payload.orderId);
  // return => ack; throw => immediate retry; ack.retry({ delay }) schedules one
});
```

## Custom providers and tests

Tests can use the built-in memory fake driver, which supports store-and-forward delivery, delays, retries, acknowledgements, and dead letters with deterministic controls:

```ts
import { createFakeQueue } from "@mohamedhabibwork/queuekit/testing";

const testQueue = createFakeQueue();
await testQueue.publish("emails", { type: "welcome", payload: { email: "person@example.com" } });

await testQueue.waitUntilIdle(); // await all in-flight handler work
await testQueue.flush(); // force delayed messages out immediately
testQueue.pause();
testQueue.resume(); // hold and release delivery
testQueue.pending("emails"); // queued, unacknowledged messages
testQueue.deadLetters("emails"); // rejected / exhausted messages
testQueue.failNext(new Error("broker down")); // inject the next publish failure
```

The fake is also a first-class driver, so `createQueue` and `createQueueManager` can point at it with the same config shape used in production:

```ts
import { createQueueManager } from "@mohamedhabibwork/queuekit";

const manager = createQueueManager({ providers: { jobs: { type: "memory" } }, default: "jobs" });
```

Custom providers are declared with `defineQueueProvider`:

```ts
import { defineQueueProvider } from "@mohamedhabibwork/queuekit/custom";
import { createMemoryQueue } from "@mohamedhabibwork/queuekit/testing";

const testQueue = createMemoryQueue();

const provider = defineQueueProvider({
  name: "internal" as const,
  capabilities: { kind: "queue", publish: true, consume: false },
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

### End-to-end tests

`tests/e2e` runs every real driver — Redis (pub/sub and streams), RabbitMQ, Kafka, NATS, SQS, and BullMQ — through a produce → consume → acknowledge round-trip against live brokers:

```sh
docker compose up -d   # redis, rabbitmq, kafka, nats, localstack (SQS)
npm test               # e2e suites run when a broker is reachable and skip otherwise
```

Broker endpoints can be overridden with the `QUEUEKIT_E2E_REDIS_URL`, `QUEUEKIT_E2E_RABBITMQ_URL`, `QUEUEKIT_E2E_KAFKA_BROKER`, `QUEUEKIT_E2E_NATS_URL`, and `QUEUEKIT_E2E_SQS_ENDPOINT` environment variables. The compose stack uses non-default local ports (Redis `6390`, RabbitMQ `5673`) so it never collides with an already-running native broker.

## Publishing

Publishing runs only from the Release workflow. Add an npm automation token as the repository Actions secret `NPM_TOKEN`; a local `.env` file cannot be read by GitHub-hosted runners. Details are in the [publishing guide](https://mohamedhabibwork.github.io/queuekit/publishing.html).

## Use with AI (llms.txt)

This repo ships an `llms.txt` — a curated, LLM-readable map of the API, semantics, and docs, written so coding assistants get it right the first time.

- **Cursor / Claude Code / Copilot**: open [`llms.txt`](https://github.com/mohamedhabibwork/queuekit/blob/main/llms.txt) or paste the raw text into your rules file (`CLAUDE.md`, `.cursorrules`, `AGENTS.md`).
- **ChatGPT / Custom GPTs / Perplexity**: add the raw URL — https://raw.githubusercontent.com/mohamedhabibwork/queuekit/main/llms.txt
- **Offline / agents in CI**: `llms.txt`, the README, and every guide in `docs/` ship inside the npm tarball, so agents can read them straight from `node_modules/@mohamedhabibwork/queuekit/`.
- **Contributing to this repo**: [AGENTS.md](AGENTS.md) documents layout, commands, and conventions for coding agents.

## License

[MIT](LICENSE)

## Logging with loggerkit

Managers accept an optional `logger` (any object with `debug/info/warn/error`), so a
[`@mohamedhabibwork/loggerkit`](https://github.com/mohamedhabibwork/loggerkit) `Logger` plugs in
directly with no extra dependency:

```ts
import { createLogger } from "@mohamedhabibwork/loggerkit";
import { createQueueManager } from "@mohamedhabibwork/queuekit";

const manager = createQueueManager({ ...config, logger: createLogger({ name: "queue" }) });
```

Provider creation and close events are logged at `debug`; creation failures at `error`.
