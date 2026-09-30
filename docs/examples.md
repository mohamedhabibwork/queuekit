# End-to-end examples

Complete, runnable producer/worker applications built with QueueKit. Each
example runs with the zero-dependency memory provider and swaps to a real
broker by changing one config object.

## 1. Email pipeline: producer API + worker

```sh
mkdir emails && cd emails && npm init -y
npm install @mohamedhabibwork/queuekit
# Production brokers are optional peers — add only the one you use:
# npm install bullmq        (Redis-backed jobs)
# npm install kafkajs       (event streaming)
node --run server # or: node server.mjs && node worker.mjs
```

```ts
// shared.mjs — one config object decides the broker for app and worker alike.
import { createQueue } from "@mohamedhabibwork/queuekit";

export function makeQueue() {
  return createQueue({
    type: "memory", // development
    // type: "bullmq", connection: { host: "127.0.0.1", port: 6379 }, // production
  });
}
```

```ts
// server.mjs — enqueue from HTTP handlers; never do slow work inline.
import express from "express";
import { makeQueue } from "./shared.mjs";

const app = express();
app.use(express.json());
const queue = await makeQueue();

app.post("/signup", async (req, res) => {
  await queue.publish(
    "emails",
    { type: "welcome", payload: { email: req.body.email } },
    { retry: { attempts: 3, backoff: { type: "exponential", delay: 1_000, maxDelay: 30_000 } } },
  );
  res.status(202).json({ queued: true });
});

app.listen(3000, () => console.log("POST /signup { email }"));
```

```ts
// worker.mjs — run in the same process for dev, or a dedicated container.
import { makeQueue } from "./shared.mjs";

const queue = await makeQueue();
const consumer = await queue.consume("emails", async ({ message }) => {
  console.log("sending welcome email to", message.payload.email);
  // return normally => acknowledged; throw => the publish retry policy applies
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, async () => {
    await consumer.close(); // stops accepting new messages, waits for in-flight work
    await queue.close();
    process.exit(0);
  });
}
```

```sh
curl -X POST localhost:3000/signup -H 'content-type: application/json' -d '{"email": "ada@example.com"}'
```

## 2. Pub/sub fan-out with pause/resume

`consume()` returns a handle — pause during deploys or backpressure, resume
after, and inspect health.

```ts
// fanout.mjs
import { createQueue } from "@mohamedhabibwork/queuekit";

const queue = await createQueue({ type: "memory" });

const consumer = await queue.consume("events", async ({ message }) => {
  console.log("got", message.type, message.payload);
});

await queue.publish("events", { type: "user.created", payload: { id: 1 } });
await queue.publish("events", { type: "user.deleted", payload: { id: 2 } });

await consumer.pause();
await queue.publish("events", { type: "user.created", payload: { id: 3 } }); // waits in the broker
await consumer.resume();

console.log((await queue.health?.())?.ok);
await consumer.close();
await queue.close();
```

## 3. Testing handlers without a broker

The memory provider and the fake inspect state directly — no mocks, no
sleeps, no Docker.

```ts
// emails.test.ts
import { expect, it } from "vitest";
import { createMemoryQueue } from "@mohamedhabibwork/queuekit";

it("delivers a welcome email and acknowledges it", async () => {
  const queue = createMemoryQueue();
  const sent: string[] = [];

  await queue.publish("emails", { type: "welcome", payload: { email: "ada@example.com" } });
  expect(queue.pending("emails")).toHaveLength(1); // consumer not attached yet

  await queue.consume("emails", async ({ message }) => {
    sent.push(message.payload.email);
  });

  expect(sent).toEqual(["ada@example.com"]);
  expect(queue.pending("emails")).toHaveLength(0);
  expect(queue.acknowledged("emails")).toHaveLength(1);
});
```

Run: `npx vitest`.

## Where to next

- [Framework integration](frameworks.md) — Express, Fastify, NestJS, Hono, Next.js, Elysia recipes.
- [Architecture](ARCHITECTURE.md) — layer map and how optional peers load on creation.
- README — the full provider table and custom-provider registration.
