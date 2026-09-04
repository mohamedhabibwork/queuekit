# @queue-kit/memory

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
