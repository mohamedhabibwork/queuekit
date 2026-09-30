# Framework integration

QueueKit is framework-agnostic — enqueueing is an async `publish()` and consuming is a plain
async loop, so it fits any HTTP framework or worker process. The recipes below cover the common
integrations.

| Framework            | Pattern used below                    |
| -------------------- | ------------------------------------- |
| Express 4/5          | enqueue from routes, worker elsewhere |
| Fastify 4/5          | plugin with graceful close            |
| NestJS 10+           | injectable `QueueService` module      |
| Hono 4               | route handler enqueue                 |
| Next.js (App Router) | server action + separate worker       |
| Elysia (Bun)         | shared async instance                 |
| Elysia (Bun)         | shared async instance                 |

The package installs with **zero dependencies**. Add only the SDK of the provider you use
(`npm install kafkajs`, `bullmq`, `ioredis`, `amqplib`, `nats`, `redis`, or
`@aws-sdk/client-sqs`) — it is loaded only when that provider is created. The in-memory provider
needs nothing and is ideal for tests.

## Express

```ts
import express from "express";
import { createQueue } from "@mohamedhabibwork/queuekit";

const app = express();
const emails = await createQueue({ type: "memory" }); // swap for bullmq/kafka in production

app.post("/signup", express.json(), async (req, res) => {
  await emails.publish("emails", { type: "welcome", payload: { email: req.body.email } });
  res.status(202).json({ queued: true });
});

app.listen(3000);
```

The same queue object is the consumer. Run it in the same process for simple apps, or in a
dedicated worker process with the provider URL pointed at the same broker:

```ts
await queue.consume("emails", async ({ message }) => {
  await sendWelcomeEmail(message.payload.email); // return => ack; throw => retry policy
});
```

## Fastify

```ts
import Fastify from "fastify";
import { createQueue } from "@mohamedhabibwork/queuekit";

const app = Fastify();
const emails = await createQueue({ type: "memory" });

app.decorate("emails", emails);
app.post("/signup", async (request, reply) => {
  await app.emails.publish("emails", { type: "welcome", payload: request.body });
  return reply.code(202).send({ queued: true });
});

// Close the queue connection with the server.
app.addHook("onClose", async () => {
  await app.emails.close();
});

await app.ready();
```

## NestJS

```ts
import { Global, Injectable, Module, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { createQueue, type MemoryConfig, type QueueForConfig } from "@mohamedhabibwork/queuekit";

@Injectable()
export class EmailsQueue implements OnModuleInit, OnModuleDestroy {
  private queue!: QueueForConfig<MemoryConfig>;

  async onModuleInit() {
    this.queue = await createQueue({ type: "memory" });
    await this.queue.consume("emails", async ({ message }) => deliver(message.payload));
  }

  enqueue(email: string) {
    return this.queue.publish("emails", { type: "welcome", payload: { email } });
  }

  async onModuleDestroy() {
    await this.queue.close();
  }
}

@Global()
@Module({ providers: [EmailsQueue], exports: [EmailsQueue] })
export class QueuesModule {}
```

## Hono

```ts
import { Hono } from "hono";
import { createQueue } from "@mohamedhabibwork/queuekit";

const emails = await createQueue({ type: "memory" });
const app = new Hono();

app.post("/signup", async (c) => {
  const { email } = await c.req.json();
  await emails.publish("emails", { type: "welcome", payload: { email } });
  return c.json({ queued: true }, 202);
});

export default app;
```

## Next.js (App Router)

```ts
// app/actions/signup.ts — server action enqueues; a worker script consumes.
"use server";
import { createQueue } from "@mohamedhabibwork/queuekit";

const emails = await createQueue({ type: "memory" });

export async function signup(email: string) {
  await emails.publish("emails", { type: "welcome", payload: { email } });
}
```

```ts
// worker.ts — start separately: npx tsx worker.ts
import { createQueue } from "@mohamedhabibwork/queuekit";

const emails = await createQueue({ type: "memory" }); // same provider config as the app
await emails.consume("emails", async ({ message }) => sendWelcomeEmail(message.payload.email));
```

> In-process providers such as `memory` only share work inside one process. For Next.js apps
> deployed across instances, point both sides at a real broker (for example `bullmq` or `kafka`).

## Elysia (Bun)

```ts
import { Elysia } from "elysia";
import { createQueue } from "@mohamedhabibwork/queuekit";

const emails = await createQueue({ type: "memory" }); // swap for a broker in production

new Elysia()
  .post("/signup", async ({ body, set }) => {
    await emails.publish("emails", { type: "welcome", payload: { email: body.email } });
    set.status = 202;
    return { queued: true };
  })
  .listen(3000);
```

## See also

- [Architecture](ARCHITECTURE.md) — layer map, provider list, and how optional peers load on creation.
- [Providers](../../README.md#providers) — every provider, its config, and its SDK.
- [Custom providers](../../README.md#custom-providers-and-tests) — register an in-process fake or your own driver.
