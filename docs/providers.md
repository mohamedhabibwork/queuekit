# Provider guide

Install the corresponding optional peer dependency before creating a provider. If it is missing, QueueKit throws a `QueueConfigError` with the exact install command.

## Acknowledgements

- BullMQ: returning from the worker completes the job; its normalized acknowledgement is a no-op.
- Kafka: `ack.complete()` commits the next offset for that message. Set `autoAck` to commit after a successful handler.
- RabbitMQ: `complete`, `retry`, and `reject` map to `ack`, `nack`, and `reject`.
- Redis Streams: `complete` maps to `XACK`; Redis Pub/Sub has no persistent acknowledgement.
- NATS Core: messages are ephemeral and acknowledgements are no-ops. JetStream publishing is available through the adapter; use `native()` for advanced pull-consumer setup in v0.1.
- SQS: `complete` deletes the message and `retry({ delay })` changes its visibility timeout.
- Memory fake (testing): `complete` records the message as acknowledged, `retry` re-queues it with an incremented `attempt`, and `reject` dead-letters it (or re-queues with `{ requeue: true }`). Handlers that throw are retried up to `setMaxAttempts()` (default 3) and then dead-lettered.

## Examples

### BullMQ jobs

```ts
import { createBullMQ } from '@mohamedhabibwork/queuekit/bullmq';

const jobs = await createBullMQ({
  type: 'bullmq',
  connection: { host: 'localhost', port: 6379 },
});

await jobs.publish('emails', {
  type: 'welcome',
  payload: { userId: 'u_1' },
}, {
  native: { attempts: 5, backoff: { type: 'exponential', delay: 1_000 } },
});
```

### Kafka streams

```ts
import { createKafka } from '@mohamedhabibwork/queuekit/kafka';

const kafka = await createKafka({
  type: 'kafka', clientId: 'billing', brokers: ['localhost:9092'],
});

await kafka.consume('payments.completed', async ({ message, ack }) => {
  await processPayment(message.payload);
  await ack.complete(); // commits the next Kafka offset
}, { native: { groupId: 'billing-workers', fromBeginning: false } });
```

### RabbitMQ queues

```ts
import { createRabbitMQ } from '@mohamedhabibwork/queuekit/rabbitmq';

const rabbit = await createRabbitMQ({ type: 'rabbitmq', url: 'amqp://localhost' });
await rabbit.publish('emails', { payload: { to: 'person@example.com' } }, {
  native: { persistent: true, priority: 5 },
});
```

### Redis Streams

```ts
import { createRedisQueue } from '@mohamedhabibwork/queuekit/redis';

const stream = await createRedisQueue({
  type: 'redis', mode: 'streams', url: 'redis://localhost:6379',
  group: 'billing', consumer: 'worker-a',
});
await stream.publish('payments', { payload: { paymentId: 'pay_1' } });
```

### Amazon SQS

```ts
import { createSqs } from '@mohamedhabibwork/queuekit/sqs';

const sqs = await createSqs({ type: 'sqs', region: 'eu-central-1' });
await sqs.publish(process.env.QUEUE_URL!, { payload: { taskId: 'task-1' } }, {
  native: { MessageGroupId: 'jobs', MessageDeduplicationId: 'task-1' },
});
```

### Memory fake (testing)

An in-memory fake driver with deterministic delivery controls, for tests. Messages published before a consumer attaches stay queued and are delivered FIFO; `waitUntilIdle()` awaits all in-flight handler work.

```ts
import { createFakeQueue } from '@mohamedhabibwork/queuekit/testing';

const fake = createFakeQueue();
await fake.consume<{ userId: string }>('emails', async ({ message, ack }) => {
  await sendWelcome(message.payload.userId);
  await ack.complete();
}, { autoAck: false });

await fake.publish('emails', { type: 'welcome', payload: { userId: 'u_1' } }, { delay: 1_000 });
await fake.waitUntilIdle();                          // handler has not run yet: message is delayed
await fake.flush();                                  // force delayed messages out now

fake.pending('emails');                              // queued / in-flight / unacknowledged
fake.acknowledged('emails');                         // settled messages
fake.deadLetters('emails');                          // rejected or exhausted messages
fake.failNext(new Error('broker down'));             // inject the next publish failure
```

The fake is also a first-class driver type, so `createQueue({ type: 'memory' })` and `createQueueManager({ providers: { jobs: { type: 'memory' } } })` work with the same config shape used in production.

## Delivery and retries

Providers determine delivery and retry behavior. The normalized `delay`, `priority`, `ttl`, and `retry` fields are convenience values only where a provider supports them. Use `native` for broker-specific knobs. Do not assume exactly-once delivery; make handlers idempotent.
