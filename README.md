# Queue Kit

[![@mohamedhabibwork/core version](https://img.shields.io/npm/v/@mohamedhabibwork/core)](https://www.npmjs.com/package/@mohamedhabibwork/core)
[![@mohamedhabibwork/memory version](https://img.shields.io/npm/v/@mohamedhabibwork/memory)](https://www.npmjs.com/package/@mohamedhabibwork/memory)
[![@mohamedhabibwork/core downloads](https://img.shields.io/npm/dm/@mohamedhabibwork/core)](https://www.npmjs.com/package/@mohamedhabibwork/core)
[![Latest Release (monorepo)](https://img.shields.io/github/v/release/mohamedhabibwork/queuekit)](https://github.com/mohamedhabibwork/queuekit/releases/latest)
[![License: MIT](https://img.shields.io/npm/l/@mohamedhabibwork/core)](./LICENSE)
[![GitHub: @mohamedhabibwork](https://img.shields.io/badge/GitHub-@mohamedhabibwork-181717?logo=github&logoColor=white)](https://github.com/mohamedhabibwork)
[![Node.js ≥ 20](https://img.shields.io/node/v/@mohamedhabibwork/core)](https://nodejs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org)
[![CI](https://github.com/mohamedhabibwork/queuekit/actions/workflows/ci.yml/badge.svg)](https://github.com/mohamedhabibwork/queuekit/actions/workflows/ci.yml)
[![release-please](https://github.com/mohamedhabibwork/queuekit/actions/workflows/release-please.yml/badge.svg)](https://github.com/mohamedhabibwork/queuekit/actions/workflows/release-please.yml)
[![CodeQL](https://github.com/mohamedhabibwork/queuekit/actions/workflows/codeql.yml/badge.svg)](https://github.com/mohamedhabibwork/queuekit/security/code-scanning)

> Unified TypeScript queues across providers, without sacrificing native provider power.

Queue Kit gives TypeScript applications one consistent API for working with multiple queue
providers while preserving each provider's native type system. Portable code stays portable and
provider-specific code stays powerful:

```ts
// 1. Portable — works on every provider
await queue.send(sendEmail, payload, { delay: "30s" });

// 2. Provider-aware — `native` is typed by the provider you selected
await queue.send(sendEmail, payload, {
  native: { attempts: 5, removeOnComplete: true }, // BullMQ options, autocompleted
});

// 3. Escape hatch — the real provider SDK object, fully typed
const bullQueue = queue.native();

// 4. Capability discovery — no silent approximation
if (queue.supports("priorities")) { /* ... */ }
```

Picking a different provider changes what `native` means — automatically:

```ts
const queue = createQueue({ name: "orders", provider: sqs({ region: "eu-west-1", queueUrl }) });

await queue.send("order.created", payload, {
  native: { MessageGroupId: "customer-42", MessageDeduplicationId: "order-123" }, // SQS options
});
// …and `native: { removeOnComplete: true }` here is a *compile-time* error.
```

## Packages

| Package | Purpose |
|---|---|
| [`@mohamedhabibwork/core`](packages/core) | Queue primitives, provider contract, worker engine, typed native escape hatches. Zero runtime dependencies. |
| [`@mohamedhabibwork/memory`](packages/memory) | In-memory provider for tests, examples and local development. Deterministic with `FakeClock`. |

Planned adapters (same contract, same native-typing rules): BullMQ, AWS SQS, RabbitMQ, Azure Queue
Storage, Google Cloud Tasks — plus community adapters via `defineQueueProvider`.

## Design rules

1. **No lowest-common-denominator API.** Portable options exist only where semantics are
   consistent across providers (`delay`, `idempotencyKey`, `correlationId`, `metadata`, `trace`,
   `concurrency`, portable retry policy, dead-lettering). Everything else stays in `native`.
2. **Native options are strongly typed.** The provider you construct determines every `native`
   surface through a generic type registry (`ProviderTypes`) — never `Record<string, unknown>`.
3. **Capability discovery instead of silent approximation.** Unsupported portable options throw
   `UnsupportedCapabilityError`; `queue.supports()` / `queue.assertCapability()` let you branch.
4. **Never discard provider errors.** Original errors are preserved on `error.cause`.
5. **Cross-runtime by construction.** ESM-only, no Node built-ins in core, standard web APIs
   (`AbortController`, `TextEncoder`, WebCrypto) — runs on Node ≥ 20, Bun and Deno.

## Quick start

```bash
pnpm add @mohamedhabibwork/core @mohamedhabibwork/memory
```

```ts
import { createQueue, defineJob } from "@mohamedhabibwork/core";
import { memory } from "@mohamedhabibwork/memory";

const sendEmail = defineJob<{ to: string; subject: string }, { messageId: string }>("email.send");

const queue = createQueue({ name: "emails", provider: memory() });

await queue.send(sendEmail, { to: "ada@example.com", subject: "Welcome" }, {
  delay: "10s",
});

const worker = queue.worker(
  sendEmail,
  async (job) => {
    console.log(`to: ${job.data.to}`);        // fully inferred
    return { messageId: `smtp-${job.id}` };   // result type checked
  },
  { concurrency: 10 },
);

await worker.close({ timeout: "30s" });       // graceful: stop fetching, drain, release
await queue.close();
```

### Deterministic tests

```ts
import { FakeClock } from "@mohamedhabibwork/core";
import { memory } from "@mohamedhabibwork/memory";

const clock = new FakeClock();
const queue = createQueue({ name: "emails", provider: memory({ clock }) });

await queue.send(sendEmail, payload, { delay: "5m" });
clock.advance("5m"); // the delayed message becomes visible — no real sleeping
```

### Typed job definitions + validation

```ts
import { defineJob } from "@mohamedhabibwork/core";
import { z } from "zod"; // any Standard Schema library works

const chargeCard = defineJob({
  name: "payment.charge",
  version: 2,
  input: z.object({ invoiceId: z.string() }), // validated before enqueue and after dequeue
});
```

### Portable retries and dead-lettering

```ts
const worker = queue.worker(chargeCard, handler, {
  retry: {
    attempts: 5,
    backoff: { strategy: "exponential", delay: "1s", maxDelay: "1m" },
    // backoff: ({ attempt, error }) => Math.min(60_000, 2 ** attempt * 1000),
    when: (error) => isNetworkError(error),   // fatal errors always skip retry
  },
  deadLetter: { queue: "payments.dlq" },      // queue-kit strategy: portable everywhere
  timeout: "2m",                              // handler timeout (≠ provider visibility timeout)
});
```

Queue Kit separates application retry (above), provider retry (native options) and delivery retry
(visibility-timeout redelivery) instead of merging them into one ambiguous `retry` number.

## Runtime support

| Runtime | Status |
|---|---|
| Node.js ≥ 20 | ✅ primary target (CI matrix) |
| Bun | ✅ smoke-tested (`bun run bun:smoke`) |
| Deno 2 | ✅ via `npm:@mohamedhabibwork/*` specifiers (ESM + standard APIs) |
| Browsers | Core types/serialization only; providers and workers are server-side |

## Development

```bash
pnpm install
pnpm typecheck      # strict TS: exactOptionalPropertyTypes, noUncheckedIndexedAccess, …
pnpm test           # unit + contract tests (vitest, with type-level tests)
pnpm test:types     # explicit typecheck pass incl. native-option proofs
pnpm build          # ESM + bundled .d.ts via tsup
pnpm bun:smoke      # run the basic example under Bun
```

### Release-blocking guarantees

- Native options of one provider are rejected by another provider's queue at compile time —
  enforced by `packages/core/test/native-types.test-d.ts`.
- No provider SDK dependency in core (core has **zero** runtime dependencies).
- Graceful shutdown, AbortSignal handling and poison-message paths are covered by tests.

## License

MIT
