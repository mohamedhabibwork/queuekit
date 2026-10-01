# Use cases

Every snippet below uses QueueKit's public API. Most run as-is against the
in-memory provider (`{ type: "memory" }`); swap the config for a real broker in
production. Provider-specific behaviour is called out where it matters.

- [1. Publish a job / message](#1-publish-a-job--message)
- [2. Consume with concurrency](#2-consume-with-concurrency)
- [3. Manual acknowledgement: complete, retry, reject](#3-manual-acknowledgement-complete-retry-reject)
- [4. Delayed and scheduled jobs](#4-delayed-and-scheduled-jobs)
- [5. Recurring jobs](#5-recurring-jobs)
- [6. Priorities, TTL and native options](#6-priorities-ttl-and-native-options)
- [7. Retries with backoff](#7-retries-with-backoff)
- [8. Dead-letter queues](#8-dead-letter-queues)
- [9. Handler timeouts](#9-handler-timeouts)
- [10. Idempotent consumers](#10-idempotent-consumers)
- [11. Deduplicated publishing](#11-deduplicated-publishing)
- [12. Batch publishing](#12-batch-publishing)
- [13. Pub/sub fan-out and streams](#13-pubsub-fan-out-and-streams)
- [14. Pause / resume (backpressure)](#14-pause--resume-backpressure)
- [15. Graceful shutdown](#15-graceful-shutdown)
- [16. Multiple brokers with a manager](#16-multiple-brokers-with-a-manager)
- [17. Typed message registry](#17-typed-message-registry)
- [18. Middleware: logging, tracing, metrics](#18-middleware-logging-tracing-metrics)
- [19. Correlation and tracing ids](#19-correlation-and-tracing-ids)
- [20. Codecs](#20-codecs)
- [21. Health checks](#21-health-checks)
- [22. Capability detection](#22-capability-detection)
- [23. Error handling](#23-error-handling)
- [24. Testing without a broker](#24-testing-without-a-broker)
- [25. Custom providers](#25-custom-providers)
- [26. Logging with loggerkit](#26-logging-with-loggerkit)

## 1. Publish a job / message

```ts
import { createQueue } from "@mohamedhabibwork/queuekit";

const queue = await createQueue({ type: "memory" });
const result = await queue.publish("emails", {
  type: "welcome",
  payload: { email: "ada@example.com" },
});
console.log(result.messageId);
```

## 2. Consume with concurrency

```ts
const consumer = await queue.consume<{ email: string }>(
  "emails",
  async ({ message }) => {
    await sendEmail(message.payload.email);
  },
  { concurrency: 5 }, // up to 5 handlers in flight
);
```

Handlers that return normally are acknowledged (`autoAck` defaults to `true`).

## 3. Manual acknowledgement: complete, retry, reject

```ts
await queue.consume(
  "payments",
  async ({ message, ack }) => {
    if (!isValid(message.payload)) return ack.reject?.({ requeue: false });
    try {
      await charge(message.payload);
      await ack.complete();
    } catch {
      await ack.retry?.({ delay: 5_000 });
    }
  },
  { autoAck: false },
);
```

`retry` and `reject` are optional: check `queue.capabilities.ack` / `nack`.

## 4. Delayed and scheduled jobs

```ts
await queue.publish("reminders", { payload: { userId: "u1" } }, { delay: 60_000 });
await queue.publish(
  "reports",
  { payload: { month: "2026-09" } },
  { scheduleAt: new Date("2026-10-02T08:00:00Z") },
);
```

Supported where `capabilities.delayed` is true (BullMQ, SQS up to 15 min,
memory). `scheduleAt` is converted to a delay relative to now; an explicit
`delay` wins.

## 5. Recurring jobs

```ts
import { scheduleRecurring } from "@mohamedhabibwork/queuekit";

const heartbeat = scheduleRecurring(
  queue,
  "metrics.flush",
  () => ({ payload: { at: Date.now() } }), // fresh message per run
  { every: 30_000, immediate: true, onError: (error) => console.error(error) },
);

// later
heartbeat.stop();
```

This is an in-process interval: run it on one instance. For cron expressions
or cluster-wide schedules use the broker's native feature, e.g. BullMQ
`native: { repeat: { pattern: "0 8 * * *" } }`.

## 6. Priorities, TTL and native options

```ts
import { createQueue } from "@mohamedhabibwork/queuekit";

const jobs = await createQueue({ type: "bullmq", connection: { host: "localhost", port: 6379 } });
await jobs.publish(
  "renders",
  { payload: { id: 1 } },
  { priority: 1, native: { removeOnComplete: true, jobId: "render-1" } },
);
```

Anything provider-specific goes under `native` and is type-checked per provider.

## 7. Retries with backoff

Provider-agnostic, driven by `ack.retry({ delay })`:

```ts
import { withRetry } from "@mohamedhabibwork/queuekit";

await queue.consume(
  "webhooks",
  withRetry(
    async ({ message }) => {
      await deliverWebhook(message.payload);
    },
    {
      attempts: 5,
      backoff: { type: "exponential", delay: 1_000, maxDelay: 60_000 },
      onFailure: (error, attempt) => console.warn(`attempt ${attempt} failed`, error),
    },
  ),
);
```

After the last attempt the message is rejected without requeue, so it lands in
the dead-letter queue. `computeBackoff(policy, attempt)` is exported if you need
the delay yourself. BullMQ users can instead use its native `attempts`/`backoff`.

## 8. Dead-letter queues

DLQs are broker features: RabbitMQ dead-letter exchanges, SQS redrive policies,
Redis Streams `native.deadLetter`, BullMQ failed set. `ack.reject({ requeue: false })`
(or `withRetry` exhaustion) routes a message there. Inspect them in tests:

```ts
import { createMemoryQueue } from "@mohamedhabibwork/queuekit/testing";

const queue = createMemoryQueue();
await queue.consume("jobs", ({ ack }) => ack.reject?.());
await queue.publish("jobs", { payload: 1 });
await queue.waitUntilIdle();
console.log(queue.deadLetters("jobs").length); // 1
```

## 9. Handler timeouts

```ts
import { withTimeout } from "@mohamedhabibwork/queuekit";

await queue.consume(
  "thumbnails",
  withTimeout(async ({ message, signal }) => {
    await fetch(message.payload.url, { signal }); // aborted on timeout
  }, 10_000),
);
```

A timeout throws `QueueTimeoutError` (retryable) — compose with `withRetry`:
`withRetry(withTimeout(handler, 10_000), { attempts: 3 })`.

## 10. Idempotent consumers

At-least-once delivery means duplicates happen. Skip already-processed messages:

```ts
import { withIdempotency, type IdempotencyStore } from "@mohamedhabibwork/queuekit";

// Shared store across workers (example with ioredis)
const store: IdempotencyStore = {
  has: async (key) => (await redis.exists(`idem:${key}`)) === 1,
  set: async (key, ttl) => void (await redis.set(`idem:${key}`, "1", "PX", ttl ?? 86_400_000)),
};

await queue.consume(
  "orders",
  withIdempotency(async ({ message }) => fulfil(message.payload), {
    store,
    key: (message) => message.headers["order-id"] as string,
    ttl: 86_400_000,
  }),
);
```

Without `store`, an in-process store (`createMemoryIdempotencyStore()`) is used
and the key defaults to `message.id`. Keys are recorded only after success.

## 11. Deduplicated publishing

```ts
await queue.publish("invoices", { payload: { id: "inv_1" } }, { idempotencyKey: "inv_1" });
await queue.publish("invoices", { payload: { id: "inv_1" } }, { idempotencyKey: "inv_1" }); // no-op
```

Honoured by drivers that support it (SQS FIFO dedup id, BullMQ job id, memory).
Otherwise pair it with an idempotent consumer.

## 12. Batch publishing

```ts
const results = await queue.publishMany?.("events", [
  { type: "a", payload: 1 },
  { type: "b", payload: 2 },
]);
```

`capabilities.batchPublish` tells you whether the broker batches natively.

## 13. Pub/sub fan-out and streams

```ts
const events = await createQueue({ type: "redis", mode: "pubsub", url: "redis://localhost:6379" });
await events.consume("user.events", ({ message }) => audit(message));
await events.consume("user.events", ({ message }) => notify(message));
await events.publish("user.events", { type: "user.created", payload: { id: 1 } });
```

Every subscriber receives each message. For durable, replayable logs use Kafka
or Redis Streams (`mode: "streams"`) with consumer groups via `native`.

## 14. Pause / resume (backpressure)

```ts
const consumer = await queue.consume("imports", handleImport);
await consumer.pause(); // stop taking new messages
await consumer.resume();
console.log(consumer.status); // "running"
```

## 15. Graceful shutdown

```ts
import { gracefulShutdown } from "@mohamedhabibwork/queuekit";

const consumer = await queue.consume("emails", handler);
gracefulShutdown({
  consumers: [consumer], // closed first: stop intake, drain in-flight work
  queues: [queue], // then connections
  timeout: 30_000, // QueueTimeoutError if draining takes longer
  onError: (error) => console.error("shutdown failed", error),
}); // listens for SIGINT/SIGTERM and exits afterwards
```

Pass `signals: []` and call `.shutdown()` yourself to integrate with a
framework's lifecycle hooks; `.dispose()` removes the listeners.

## 16. Multiple brokers with a manager

```ts
import { createQueueManager } from "@mohamedhabibwork/queuekit";

const queues = createQueueManager({
  default: "jobs",
  providers: {
    jobs: { type: "bullmq", connection: { host: "localhost", port: 6379 } },
    events: { type: "kafka", clientId: "api", brokers: ["localhost:9092"] },
  },
});
await queues.warmup(); // optional: connect eagerly
const events = await queues.provider("events"); // lazily created, cached, fully typed
await queues.close();
```

## 17. Typed message registry

```ts
import { createQueue, createTypedQueue } from "@mohamedhabibwork/queuekit";

type Messages = { "user.welcome": { email: string }; "order.paid": { orderId: string } };
const typed = createTypedQueue<Messages>(await createQueue({ type: "memory" }));
await typed.publish("user.welcome", { email: "ada@example.com" });
await typed.consume("order.paid", ({ message }) => console.log(message.payload.orderId));
```

## 18. Middleware: logging, tracing, metrics

```ts
const queue = await createQueue({ type: "memory" });
queue.use(async (context, next) => {
  const started = performance.now();
  try {
    await next();
  } finally {
    console.log(context.operation, context.destination, performance.now() - started);
  }
});
```

## 19. Correlation and tracing ids

```ts
await queue.publish("orders", {
  payload: { id: 1 },
  correlationId: request.id,
  traceId: span.traceId,
  headers: { tenant: "acme" },
  metadata: { local: true }, // app-only, never sent to the broker
});
```

## 20. Codecs

```ts
import { jsonCodec, textCodec, bytesCodec } from "@mohamedhabibwork/queuekit";

const encoded = jsonCodec.encode({ ok: true });
const decoded = jsonCodec.decode(encoded);
```

Serialization failures throw `QueueSerializationError` / `QueueDeserializationError`.

## 21. Health checks

```ts
app.get("/health/queues", async () => await queues.health()); // { jobs: { ok, provider }, ... }
```

## 22. Capability detection

```ts
if (!queue.capabilities.delayed) throw new Error("This broker cannot delay messages");
```

## 23. Error handling

```ts
import { QueuePublishError, isRetryableQueueError } from "@mohamedhabibwork/queuekit";

try {
  await queue.publish("emails", { payload: {} });
} catch (error) {
  if (isRetryableQueueError(error)) scheduleLater();
  else if (error instanceof QueuePublishError) console.error(error.context);
  else throw error;
}
```

## 24. Testing without a broker

```ts
import { createFakeQueue } from "@mohamedhabibwork/queuekit/testing";

const queue = createFakeQueue();
await queue.publish("emails", { payload: { email: "a@b.c" } }, { delay: 60_000 });
await queue.consume("emails", handler);
await queue.flush(); // deliver delayed messages now
expect(queue.acknowledged("emails")).toHaveLength(1);
queue.failNext(new Error("broker down")); // inject a publish failure
```

## 25. Custom providers

```ts
import { defineQueueProvider } from "@mohamedhabibwork/queuekit/custom";

export const myBroker = defineQueueProvider({
  name: "my-broker" as const,
  capabilities: { kind: "queue", publish: true, consume: true },
  async create(config: { url: string }) {
    return new MyBrokerProvider(config); // implements QueueProvider
  },
});
```

See [custom providers](custom-providers.html).

## 26. Logging with loggerkit

```ts
import { createLogger } from "@mohamedhabibwork/loggerkit";
import { createQueueManager } from "@mohamedhabibwork/queuekit";

const queues = createQueueManager({
  providers: { jobs: { type: "memory" } },
  logger: createLogger({ name: "queue" }),
});
```
