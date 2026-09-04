# @queue-kit/memory

[![npm version](https://img.shields.io/npm/v/@queue-kit/memory)](https://www.npmjs.com/package/@queue-kit/memory)
[![npm downloads](https://img.shields.io/npm/dm/@queue-kit/memory)](https://www.npmjs.com/package/@queue-kit/memory)
[![License: MIT](https://img.shields.io/npm/l/@queue-kit/memory)](./LICENSE)
[![GitHub: @mohamedhabibwork](https://img.shields.io/badge/GitHub-@mohamedhabibwork-181717?logo=github&logoColor=white)](https://github.com/mohamedhabibwork)
[![Node.js ≥ 20](https://img.shields.io/node/v/@queue-kit/memory)](https://nodejs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org)
[![CI](https://github.com/mohamedhabibwork/queuekit/actions/workflows/ci.yml/badge.svg)](https://github.com/mohamedhabibwork/queuekit/actions/workflows/ci.yml)
[![Socket](https://badge.socket.dev/npm/package/@queue-kit/memory)](https://socket.dev/npm/package/@queue-kit/memory)

In-memory Queue Kit provider for **unit tests, examples and local development**. Not durable —
never use it in production.

```bash
pnpm add @queue-kit/core @queue-kit/memory
```

```ts
import { createQueue, FakeClock } from "@queue-kit/core";
import { memory } from "@queue-kit/memory";

const clock = new FakeClock();
const queue = createQueue({ name: "emails", provider: memory({ clock }) });
```

## Supported features

Send, batch send, receive, workers (concurrency, pause/resume, graceful shutdown), delays,
priorities (`native: { priority }` — higher first), visibility timeouts with expiry redelivery and
auto-extension, ack/nack, portable retries with backoff, dead-lettering, poison-message handling,
idempotency-key deduplication (`SendResult.deduplication`), health/size/purge, and a typed test
inspector via `queue.native()`.

## Deterministic time

Pass a `FakeClock` and delays, retries and visibility leases only move when you advance the clock:

```ts
await queue.send("job", data, { delay: "5m" });
clock.advance("5m");
```

Every behaviour above is covered by the contract test-suite in this package, which doubles as the
reference for future adapters (BullMQ, SQS, RabbitMQ, …).
