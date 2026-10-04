# AGENTS.md

Guidance for AI coding agents (and humans) working in this repository.

## What this is

**QueueKit** (`@mohamedhabibwork/queuekit`) — runtime-neutral queues, messaging, pub/sub, and
streams (Kafka, RabbitMQ, BullMQ, Redis, NATS, SQS, Cloudflare Queues, Google Cloud Pub/Sub,
Azure Service Bus) behind one typed contract. Zero runtime
dependencies; provider SDKs are optional peers loaded only when that provider is created; a
full in-memory provider covers dev and tests. Node >= 20, Bun, Deno; dual ESM/CJS.

## Layout

- `src/core/` — base provider, codec, errors, lifecycle, `load-optional`, types (leaf layer).
- `src/drivers/<provider>.ts` — one file per provider.
- `src/testing/memory-queue.ts` — in-memory reference implementation.
- Root `src/*.ts` — composition: index, config, errors, factory, manager, registry, facades.
- `tests/` — Vitest suites including `architecture.test.ts` (boundary guard) and
  `peer-dependencies.test.ts` (optional-peer contract); `tests/e2e/` needs live brokers
  (skipped locally), `tests/runtime/` needs Bun/Deno.
- `docs/` — markdown guides, shipped in the npm tarball and formatted by oxfmt.

## Commands

```sh
npm ci             # install exactly the lockfile (dev deps only)
npm run check      # format:check -> lint -> build -> test -> type tests  (the gate)
npm run lint:fix   # oxlint --fix
npm run format     # oxfmt (also formats docs/*.md)
npm test           # vitest run (unit; e2e skips without brokers)
npm run test:e2e   # against docker-compose.yml services
```

CI fails on any lint warning (`--deny-warnings`), any formatting diff, or any architecture
violation. Run `npm run check` before declaring anything done.

## Conventions

- **Formatting is oxfmt, not opinion**: never hand-format; run `npm run format`.
- **Lint**: `.oxlintrc.json` enables correctness/suspicious/perf across typescript, unicorn,
  import, promise plugins. For intentional code, use an inline
  `// oxlint-disable-next-line <rule>` with a reason (see `src/drivers/nats.ts` for the
  pattern) — never widen the config for one site.
- **Architecture**: `tests/architecture.test.ts` enforces layering (core is provider-free;
  drivers import core, root `config.ts`, and their own file only; testing never imports
  drivers). New edges require updating the test AND `docs/ARCHITECTURE.md` together.
- **Dependencies**: no runtime dependencies. Provider SDKs are optional peers loaded with
  `loadOptional` and must throw `QueueConfigError` with the install command when missing
  (covered by `tests/peer-dependencies.test.ts`).
- **TypeScript**: `lib: ["ES2023", "DOM"]` in tsconfig.json; `verbatimModuleSyntax` on.
- **Docs**: README, `llms.txt`, and `docs/*.md` are part of the deliverable; API changes
  update all three.

## Gotchas

- The manager is `createQueueManager({ providers, default })` — synchronous factory, async
  `manager.provider(name)`.
- `createTypedQueue(registry)` keys destinations off a message registry map.
- Handlers: return normally => ack; throw => retry policy; manual `ack.complete()` /
  optional `ack.retry()` / `ack.reject()` where the provider supports it.
