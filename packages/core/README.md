# @mohamedhabibwork/core

[![npm version](https://img.shields.io/npm/v/@mohamedhabibwork/core)](https://www.npmjs.com/package/@mohamedhabibwork/core)
[![npm downloads](https://img.shields.io/npm/dm/@mohamedhabibwork/core)](https://www.npmjs.com/package/@mohamedhabibwork/core)
[![License: MIT](https://img.shields.io/npm/l/@mohamedhabibwork/core)](./LICENSE)
[![GitHub: @mohamedhabibwork](https://img.shields.io/badge/GitHub-@mohamedhabibwork-181717?logo=github&logoColor=white)](https://github.com/mohamedhabibwork)
[![Node.js ≥ 20](https://img.shields.io/node/v/@mohamedhabibwork/core)](https://nodejs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org)
[![CI](https://github.com/mohamedhabibwork/queuekit/actions/workflows/ci.yml/badge.svg)](https://github.com/mohamedhabibwork/queuekit/actions/workflows/ci.yml)
[![Socket](https://badge.socket.dev/npm/package/@mohamedhabibwork/core)](https://socket.dev/npm/package/@mohamedhabibwork/core)

Portable queue primitives for TypeScript with **typed native escape hatches** and **capability
discovery**. Zero runtime dependencies, ESM-only, runs on Node, Bun and Deno.

```bash
pnpm add @mohamedhabibwork/core
```

## The four usage levels

```ts
import { createQueue, defineJob } from "@mohamedhabibwork/core";
import { memory } from "@mohamedhabibwork/memory"; // any @mohamedhabibwork/* provider

const queue = createQueue({ name: "emails", provider: memory() });
const sendEmail = defineJob<{ to: string }, { id: string }>("email.send");

// 1. Portable — identical on every provider
await queue.send(sendEmail, { to: "ada@example.com" }, { delay: "30s" });

// 2. Provider-aware — `native` is the selected provider's own options, strongly typed
await queue.send(sendEmail, { to: "ada@example.com" }, { native: { /* provider options */ } });

// 3. Escape hatch — the provider SDK object, typed via ProviderTypes["nativeClient"]
queue.native();

// 4. Capability discovery — loud instead of silent
queue.supports("delayedDelivery"); // boolean
queue.assertCapability("fifo");    // throws UnsupportedCapabilityError
```

## Public surface (v0.1)

`createQueue`, `defineJob`, `queue.send/dispatch`, `queue.sendBatch`, `queue.worker`,
`queue.schedule`, `queue.native/withNative`, `queue.capabilities/supports/assertCapability`,
`queue.describe/health/size/purge`, `queue.use/on`, `queue.connect/close`,
`createQueueRegistry`, `installGracefulShutdown`, the full error taxonomy
(`QueueKitError` + 14 subclasses, `retryable()`/`fatal()` helpers), `FakeClock`,
`JsonSerializer`, and testing utilities under `@mohamedhabibwork/core/testing`
(`captureJobs`, `waitForQueueEvent`, `waitForWorkerEvent`).

## Writing a provider adapter

Adapters implement `QueueAdapter<TTypes>` where `TTypes extends ProviderTypes` carries every
native surface:

```ts
import { defineQueueProvider, type ProviderTypes } from "@mohamedhabibwork/core";

interface MyTypes extends ProviderTypes {
  connection: MyConnectionOptions;
  send: MySendOptions;          // shows up in queue.send(..., { native })
  worker: MyWorkerOptions;      // shows up in queue.worker(..., { native })
  nativeMessage: MyDeliveryHandle;
  nativeClient: MySdkClient;
  // ...
}

export const myProvider = defineQueueProvider(
  (config) => ({ id: "my-provider", capabilities, adapter, connection: config.connection, lazy: true }),
);
```

Rules for adapters:

- Preserve provider errors on `cause` — never swallow them.
- Declare capabilities honestly; core turns unsupported portable options into errors.
- Expose the SDK via `nativeClient()` for anything you don't wrap.
- The portable worker engine (`WorkerEngine`) is exported for reuse: poll → decode → validate →
  middleware → handler → ack / retry / dead-letter, with graceful shutdown and visibility
  extension built in.

## Strictness

Compiled under `strict`, `exactOptionalPropertyTypes`, `noUncheckedIndexedAccess`,
`verbatimModuleSyntax`, `isolatedModules`. No public `any`.
